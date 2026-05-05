/**
 * Frogeye MCP Server
 *
 * Supports two transports:
 *  1. StdioServerTransport — for Claude Code (claude mcp add frogeye --stdio ...)
 *  2. SSEServerTransport   — for Claude Code HTTP mode (claude mcp add -t http ...)
 *
 * CRITICAL: NEVER use console.log() — it writes to stdout and corrupts the JSON-RPC
 * message stream when using stdio transport. All logging MUST use process.stderr.write().
 */

import { Server } from '@modelcontextprotocol/sdk/server/index.js';
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js';
import { SSEServerTransport } from '@modelcontextprotocol/sdk/server/sse.js';
import { StreamableHTTPServerTransport } from '@modelcontextprotocol/sdk/server/streamableHttp.js';
import {
  CallToolRequestSchema,
  ListToolsRequestSchema,
} from '@modelcontextprotocol/sdk/types.js';
import { Redis } from '@upstash/redis';
import express from 'express';
import fetch from 'node-fetch';
import pg from 'pg';
import crypto from 'crypto';
import fs from 'fs';

const { Pool } = pg;

// ─── Configuration ────────────────────────────────────────────────────────────

const PORT = parseInt(process.env.PORT || '8080', 10);
const DATABASE_URL = process.env.DATABASE_URL;
const REDIS_URL = process.env.UPSTASH_REDIS_REST_URL;
const REDIS_TOKEN = process.env.UPSTASH_REDIS_REST_TOKEN;
const HISOLO_PROXY_URL = process.env.HISOLO_PROXY_URL || 'http://10.0.0.9:3100';
const HISOLO_API_TOKEN = process.env.HISOLO_API_TOKEN;
const NODE_ENV = process.env.NODE_ENV || 'production';

// Frogeye API base URL — used by frogeye_register to call the Next.js route
const FROGEYE_API_URL = process.env.FROGEYE_API_URL || 'https://frogeye.ai';

// ─── Logging (stderr only — stdout is reserved for MCP JSON-RPC) ──────────────

function log(level, message, data) {
  const entry = {
    ts: new Date().toISOString(),
    level,
    msg: message,
    ...(data ? { data } : {}),
  };
  process.stderr.write(JSON.stringify(entry) + '\n');
}

const logger = {
  info: (msg, data) => log('info', msg, data),
  warn: (msg, data) => log('warn', msg, data),
  error: (msg, data) => log('error', msg, data),
  debug: (msg, data) => NODE_ENV === 'development' ? log('debug', msg, data) : undefined,
};

// ─── Database Pool ────────────────────────────────────────────────────────────

/**
 * isDatabaseConfigured — set to false when DATABASE_URL is missing.
 * All MCP tools check this flag and return a structured error instead of
 * crashing, allowing Cloud Run healthchecks to pass before Neon is wired.
 */
let isDatabaseConfigured = false;
let pool = null;

if (!DATABASE_URL) {
  logger.warn('DATABASE_URL not set — MCP tools will return degraded-mode errors until configured');
} else {
  isDatabaseConfigured = true;
  pool = new Pool({
    connectionString: DATABASE_URL,
    max: 10,
    idleTimeoutMillis: 30000,
    connectionTimeoutMillis: 15000,
    ssl: DATABASE_URL.includes('localhost') ? false : { rejectUnauthorized: false },
  });

  pool.on('error', (err) => {
    logger.error('Unexpected DB pool error', { message: err.message });
  });
}

async function query(text, params) {
  if (!isDatabaseConfigured || !pool) {
    throw new Error('Database not configured. Set DATABASE_URL environment variable.');
  }
  const client = await pool.connect();
  try {
    const result = await client.query(text, params);
    return result;
  } finally {
    client.release();
  }
}

/**
 * Returns a structured MCP error response when the database is not configured.
 * Used by all tools to fail gracefully before Neon is provisioned.
 */
function dbNotConfiguredResponse() {
  return {
    content: [
      {
        type: 'text',
        text: JSON.stringify({
          type: 'error',
          message: 'Database not configured. Set DATABASE_URL environment variable.',
          hint: 'Provision a Neon PostgreSQL database and set DATABASE_URL in your Cloud Run environment.',
        }),
      },
    ],
    isError: true,
  };
}

// ─── Redis Rate Limiter ───────────────────────────────────────────────────────

let redis = null;
if (REDIS_URL && REDIS_TOKEN) {
  redis = new Redis({ url: REDIS_URL, token: REDIS_TOKEN });
  logger.info('Upstash Redis connected');
} else {
  logger.warn('UPSTASH_REDIS_REST_URL/TOKEN not set — rate limiting disabled');
}

const ANON_DAILY_LIMIT = 25;

/**
 * Returns the monthly query quota for a given tier.
 * Returns -1 for unlimited tiers (apex, admin).
 */
function getQuotaForTier(tier) {
  if (tier === 'frog') return 500;
  if (tier === 'apex' || tier === 'admin') return -1; // unlimited
  return 50; // free and unknown tiers
}

/**
 * Check and increment rate limit for an anonymous user by IP.
 * Key format: anon:{ip}:daily
 * TTL: 86400s (rolling 24h window, not calendar-day reset — acceptable for v1).
 * @returns {ok: boolean, used: number, limit: number}
 */
async function checkAnonRateLimit(ip) {
  if (!redis) {
    // Rate limiting disabled — always allow
    return { ok: true, used: 0, limit: ANON_DAILY_LIMIT };
  }

  // Take first IP from x-forwarded-for list (e.g. "1.2.3.4, proxy-ip" → "1.2.3.4")
  const cleanIp = (ip || 'unknown').split(',')[0].trim();
  const key = `anon:${cleanIp}:daily`;

  try {
    const used = await redis.incr(key);
    // Set TTL only on first use (rolling 24h window)
    if (used === 1) {
      await redis.expire(key, 86400);
    }
    if (used > ANON_DAILY_LIMIT) {
      logger.warn('Anonymous rate limit exceeded', { ip: cleanIp, used, limit: ANON_DAILY_LIMIT });
      return { ok: false, used, limit: ANON_DAILY_LIMIT };
    }
    return { ok: true, used, limit: ANON_DAILY_LIMIT };
  } catch (err) {
    // Redis failure must not block usage — log and allow
    logger.error('Redis anon rate limit check failed — allowing request', { message: err.message, ip: cleanIp });
    return { ok: true, used: 0, limit: ANON_DAILY_LIMIT };
  }
}

/**
 * Check and increment rate limit for a user.
 * Key format: ratelimit:user:{userId}:month:{YYYY-MM}
 * @param {string} userId
 * @param {string} tier — user tier: 'free' | 'frog' | 'apex' | 'admin'
 * @returns {ok: boolean, used: number, limit: number, resetAt: string}
 */
async function checkRateLimit(userId, tier) {
  const monthlyLimit = getQuotaForTier(tier);

  // Unlimited tiers (apex, admin) — skip Redis entirely
  if (monthlyLimit === -1) {
    return { ok: true, used: 0, limit: -1, resetAt: null };
  }

  if (!redis) {
    // Rate limiting disabled — always allow
    return { ok: true, used: 0, limit: monthlyLimit, resetAt: null };
  }

  const now = new Date();
  const monthKey = `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, '0')}`;
  const key = `ratelimit:user:${userId}:month:${monthKey}`;

  logger.info('checkRateLimit: incrementing quota', { userId, tier, key });

  try {
    // INCR is atomic — safe for concurrent calls
    const used = await redis.incr(key);

    // Structured quota write confirmation — used by Cloud Logging queries to verify counter health
    logger.info('QUOTA_WRITE', { key, count: used, tier, userId, limit: monthlyLimit });
    logger.info('checkRateLimit: quota incremented', { userId, tier, key, used, limit: monthlyLimit });

    // Set expiry on first use (TTL ~35 days to cover month rollover)
    if (used === 1) {
      await redis.expire(key, 35 * 24 * 60 * 60);
    }

    // Compute reset date (first of next month, UTC midnight)
    const nextMonth = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth() + 1, 1));
    const resetAt = nextMonth.toISOString();

    if (used > monthlyLimit) {
      logger.warn('Rate limit exceeded', { userId, tier, used, limit: monthlyLimit });
      return { ok: false, used, limit: monthlyLimit, resetAt };
    }

    return { ok: true, used, limit: monthlyLimit, resetAt };
  } catch (err) {
    // Redis failure must not block tool usage — log and allow
    logger.error('Redis rate limit check failed — allowing request', { message: err.message, userId });
    return { ok: true, used: 0, limit: monthlyLimit, resetAt: null };
  }
}

// ─── API Key Authentication ───────────────────────────────────────────────────

/**
 * Hash an API key using SHA-256. Keys are stored as hashes — never plaintext.
 */
function hashApiKey(key) {
  return crypto.createHash('sha256').update(key).digest('hex');
}

/**
 * Validate an API key from the X-API-Key header.
 * Returns the user record if valid, null if invalid.
 * Includes agent_username so first-call hook can check registration status.
 */
async function authenticateApiKey(rawKey) {
  if (!rawKey || typeof rawKey !== 'string' || rawKey.trim() === '') {
    return null;
  }

  const keyHash = hashApiKey(rawKey.trim());

  try {
    const result = await query(
      `SELECT ak.user_id, ak.tier, ak.queries_used, ak.quota_limit, ak.reset_at,
              ak.agent_username,
              u.email
       FROM api_keys ak
       JOIN users u ON u.id = ak.user_id
       WHERE ak.key_hash = $1`,
      [keyHash]
    );

    if (result.rows.length === 0) {
      return null;
    }

    // Attach the raw API key so frogeye_register can forward it to the REST API
    return { ...result.rows[0], rawApiKey: rawKey.trim() };
  } catch (err) {
    logger.error('API key auth DB error', { message: err.message });
    return null;
  }
}

/**
 * Express middleware — resolves API key auth and attaches req.authUser.
 *
 * Authenticated: sets req.authUser from the api_keys + users tables.
 * Anonymous:     sets req.authUser with tier='anonymous' and IP for rate limiting.
 * Invalid key:   responds 401 immediately, does not call next().
 * DB missing:    responds 503, does not call next().
 *
 * Apply as: app.get('/sse', validateApiKey, handler)
 */
async function validateApiKey(req, res, next) {
  if (!isDatabaseConfigured) {
    res.status(503).json({ error: 'Database not configured' });
    return;
  }

  const rawApiKey = req.headers['x-api-key'] || req.headers['authorization']?.replace(/^Bearer\s+/i, '');

  if (rawApiKey) {
    const authenticated = await authenticateApiKey(rawApiKey);
    if (!authenticated) {
      res.status(401).json({ error: 'Invalid API key' });
      return;
    }
    req.authUser = authenticated;
    logger.info('SSE connection authenticated', { userId: req.authUser.user_id, tier: req.authUser.tier });
  } else {
    const rawIp = req.headers['x-forwarded-for'] || req.socket.remoteAddress || 'unknown';
    const ip = String(rawIp).split(',')[0].trim();
    req.authUser = { tier: 'anonymous', userId: null, user_id: null, ip, agent_username: null };
    logger.info('SSE connection anonymous', { ip });
  }

  next();
}

// ─── First-Call Hook ──────────────────────────────────────────────────────────

/**
 * Wraps a tool result to append a registration prompt when the agent
 * hasn't claimed a username yet.
 */
function withRegistrationPrompt(result, authUser) {
  // Anonymous users and already-registered users get no registration prompt
  if (authUser.tier === 'anonymous' || authUser.agent_username) {
    return result;
  }

  try {
    const parsed = JSON.parse(result.content[0].text);
    parsed.registration_prompt =
      "You haven't claimed a username yet. Call frogeye_register(username='your-name') to identify your agent's posts.";
    return {
      ...result,
      content: [{ type: 'text', text: JSON.stringify(parsed, null, 2) }],
    };
  } catch {
    // If the result isn't parseable JSON, append as a second content block
    return {
      ...result,
      content: [
        ...result.content,
        {
          type: 'text',
          text: "Registration prompt: You haven't claimed a username yet. Call frogeye_register(username='your-name') to identify your agent's posts.",
        },
      ],
    };
  }
}

// ─── Embeddings via HISOLO Proxy ──────────────────────────────────────────────

/**
 * Generate a text embedding via the HISOLO Gemini proxy.
 * Returns a float array (vector of 768 dimensions for text-embedding-004).
 */
async function generateEmbedding(text) {
  if (!HISOLO_API_TOKEN) {
    throw new Error('HISOLO_API_TOKEN not set — cannot generate embeddings');
  }

  const truncated = text.slice(0, 8192); // Gemini embedding max input

  const response = await fetch(
    `${HISOLO_PROXY_URL}/api/v1/embed`,
    {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${HISOLO_API_TOKEN}`,
      },
      body: JSON.stringify({
        input: truncated,
        model: 'text-embedding-004',
      }),
      signal: AbortSignal.timeout(12000), // 12s timeout
    }
  );

  if (!response.ok) {
    const body = await response.text().catch(() => '<unreadable>');
    throw new Error(`Embedding API error ${response.status}: ${body}`);
  }

  const data = await response.json();

  const values = data?.data?.[0]?.embedding;
  if (!Array.isArray(values) || values.length === 0) {
    throw new Error('Embedding API returned empty values');
  }

  return values;
}

/**
 * Format a float array as a pgvector-compatible string: '[0.1,0.2,...]'
 */
function formatVector(embedding) {
  return '[' + embedding.join(',') + ']';
}

// ─── Input Normalization ──────────────────────────────────────────────────────

/**
 * Normalize a code snippet before embedding to improve detection accuracy.
 * Reduces surface-area differences between real vulnerabilities and KB patterns.
 *
 * Steps (in order):
 *   1. Strip line comments (// and #)
 *   2. Strip block comments (/* *\/)
 *   3. Strip import/require lines
 *   4. Normalize common identifiers to canonical placeholders
 *   5. Lowercase
 *   6. Truncate to 4096 chars max (Gemini embedding sweet-spot)
 */
function normalizeSnippet(text) {
  if (!text || typeof text !== 'string') return '';

  let s = text;

  // Strip single-line comments (// ... and # ...)
  s = s.replace(/\/\/[^\n]*/g, '');
  s = s.replace(/#[^\n]*/g, '');

  // Strip block comments /* ... */
  s = s.replace(/\/\*[\s\S]*?\*\//g, '');

  // Strip import/require lines
  s = s.replace(/^[ \t]*(import|require)\b[^\n]*/gm, '');

  // Normalize common variable names to canonical placeholders
  // Order matters: more specific patterns before generic ones
  s = s
    .replace(/\breq\.body\b/g, 'REQ_BODY')
    .replace(/\brequest\.body\b/g, 'REQ_BODY')
    .replace(/\buserInput\b/g, 'INPUT')
    .replace(/\buser_input\b/g, 'INPUT')
    .replace(/\binput\b/g, 'INPUT')
    .replace(/\bdb\b/g, 'DB')
    .replace(/\bdatabase\b/g, 'DB')
    .replace(/\bconn\b/g, 'CONN')
    .replace(/\bconnection\b/g, 'CONN')
    .replace(/\bpool\b/g, 'CONN');

  // Lowercase
  s = s.toLowerCase();

  // Collapse excess whitespace (multiple blank lines → single newline)
  s = s.replace(/\n{3,}/g, '\n\n').trim();

  // Truncate to 4096 chars
  return s.slice(0, 4096);
}

// ─── Tool: frogeye_search ─────────────────────────────────────────────────────

async function toolFrogeyeSearch({ query: queryArg, pattern, q, language, context, filename }, authUser) {
  // Accept 'query' (canonical), 'pattern' (legacy), or 'q' (shorthand)
  const q_input = queryArg || pattern || q || '';
  // FIX 2: Filename guard — placeholder/template files contain fake credentials, not real secrets.
  // Return known:false immediately without consuming rate limit or burning embedding quota.
  const PLACEHOLDER_EXTENSIONS = ['.example', '.sample', '.template', '.placeholder', '.stub'];
  if (filename && typeof filename === 'string') {
    const lowerFilename = filename.toLowerCase();
    const isPlaceholderFile = PLACEHOLDER_EXTENSIONS.some((ext) => lowerFilename.endsWith(ext));
    if (isPlaceholderFile) {
      logger.info('frogeye_search: placeholder filename detected — returning known:false', {
        userId: authUser.user_id,
        filename,
      });
      return {
        results: [],
        known: false,
        reason: 'placeholder_file',
        query_info: { queries_used_this_month: null, monthly_limit: null },
      };
    }
  }

  // Rate limit check — anonymous users get 25/day by IP, authenticated get monthly quota
  let rateCheck;
  if (authUser.tier === 'anonymous') {
    rateCheck = await checkAnonRateLimit(authUser.ip);
    if (!rateCheck.ok) {
      return {
        known: false,
        quota_exceeded: true,
        error: `Anonymous daily limit reached (${rateCheck.limit} queries/day).`,
        upgrade_message: "You've used your 25 free queries today. Upgrade to Frogeye Frog ($15/mo) for 500 queries/month + team memory.",
        upgrade_url: 'https://frogeye.ai/upgrade?ref=mcp-quota',
        reset_time: 'midnight UTC',
        used: rateCheck.used,
        limit: rateCheck.limit,
        auth_hint: 'Sign up free at frogeye.ai to get 50 queries/month and save your history',
      };
    }
  } else {
    rateCheck = await checkRateLimit(authUser.user_id, authUser.tier);
    if (!rateCheck.ok) {
      return {
        known: false,
        quota_exceeded: true,
        error: `Monthly query limit reached (${rateCheck.limit} queries/month).`,
        upgrade_message: "You've used your 50 free queries this month. Upgrade to Frogeye Frog ($15/mo) for 500 queries/month + team memory.",
        upgrade_url: 'https://frogeye.ai/upgrade?ref=mcp-quota',
        reset_time: rateCheck.resetAt,
        used: rateCheck.used,
        limit: rateCheck.limit,
      };
    }
  }

  // Build search text from inputs
  const searchText = [
    q_input,
    language ? `language: ${language}` : '',
    context || '',
  ].filter(Boolean).join('\n');

  // Generate embedding
  let embedding;
  try {
    embedding = await generateEmbedding(searchText);
  } catch (err) {
    logger.error('frogeye_search: embedding failed', { message: err.message });
    throw new Error(`Embedding generation failed: ${err.message}`);
  }

  const vectorStr = formatVector(embedding);

  // pgvector similarity search — ORDER BY and LIMIT are MANDATORY (HNSW activation + OOM prevention)
  // Similarity threshold (0.75) filters low-confidence matches and eliminates false positives
  // is_false_positive = true patterns are DB-level excluded (publishable keys, known safe patterns)
  const SIMILARITY_THRESHOLD = 0.75;
  const result = await query(
    `SELECT
       pattern_id,
       vuln_class,
       stack,
       severity,
       anonymized_snippet,
       fix_snippet,
       post_count,
       1 - (embedding <=> $1::vector) AS similarity
     FROM patterns
     WHERE (is_public = true OR ($3::text IS NOT NULL AND team_id = $3))
       AND embedding IS NOT NULL
       AND is_false_positive = false
       AND 1 - (embedding <=> $1::vector) > $2
     ORDER BY embedding <=> $1::vector
     LIMIT 5`,
    [vectorStr, SIMILARITY_THRESHOLD, authUser.tier !== 'anonymous' && authUser.rawApiKey
      ? crypto.createHash('sha256').update(authUser.rawApiKey).digest('hex')
      : null]
  );

  // NOTE: queries_used counter is authoritative in Redis (checkRateLimit INCR above).
  // DB writes removed to prevent permanent Redis/DB divergence (FRG-044).

  logger.info('frogeye_search: success', {
    userId: authUser.user_id,
    tier: authUser.tier,
    rawHits: result.rows.length,
    threshold: SIMILARITY_THRESHOLD,
    rateUsed: rateCheck.used,
  });

  // ── Input-side safe-list: detect safe patterns in the query itself ────────────
  // Stripe publishable keys (pk_live_, pk_test_) are intentionally public.
  // Certificate fingerprints (sha256//) are public identifiers, not secrets.
  // .env.example / template files contain placeholder values, not real credentials.
  // If the input itself is a safe pattern, return known: false immediately.
  const INPUT_SAFE_PREFIXES = [
    'pk_live_',
    'pk_test_',
    'NEXT_PUBLIC_STRIPE_PUBLISHABLE',
    'VITE_APP_STRIPE_PUBLIC',
  ];
  const TEMPLATE_MARKERS = [
    'your-256-bit-secret-key-here',
    'your-secret-key-here',
    'your-openai-api-key-here',
    'sk_test_your_secret_key_here',
    'your-supabase-anon-key-here',
    'your-smtp-password-here',
    '-key-here',
    '-secret-here',
    'change-me',
    'placeholder',
  ];
  const CERT_PIN_MARKERS = ['sha256//', 'PINNED_PUBLIC_KEYS'];

  const inputIsSafePublicKey =
    INPUT_SAFE_PREFIXES.some((p) => q_input.includes(p)) &&
    !q_input.includes('sk_live_') &&
    !q_input.includes('STRIPE_SECRET') &&
    !q_input.includes('sk_test_');

  const inputIsTemplate =
    TEMPLATE_MARKERS.some((m) => q_input.includes(m)) &&
    !q_input.includes('sk_live_') &&
    !q_input.includes('AKIA');  // Real AWS key prefix — not a placeholder

  const inputIsCertPin = CERT_PIN_MARKERS.some((m) => q_input.includes(m));

  if (inputIsSafePublicKey || inputIsTemplate || inputIsCertPin) {
    logger.info('frogeye_search: input matched safe-list — returning known:false', {
      userId: authUser.user_id,
      reason: inputIsSafePublicKey ? 'safe_public_key' : inputIsTemplate ? 'template_placeholder' : 'cert_pin',
    });
    return {
      results: [],
      known: false,
      query_info: {
        queries_used_this_month: rateCheck.used,
        monthly_limit: rateCheck.limit,
      },
    };
  }

  // ── Result-side safe-list: strip result snippets that are safe public keys ───
  // Catches cases where the knowledge graph contains "False Positive" informational
  // patterns about publishable keys.
  const RESULT_SAFE_PREFIXES = [
    'pk_live_',
    'pk_test_',
    'NEXT_PUBLIC_STRIPE_PUBLISHABLE',
    'VITE_APP_STRIPE_PUBLIC',
  ];
  const filteredRows = result.rows.filter((row) => {
    const snip = row.anonymized_snippet ?? '';
    const vc = row.vuln_class ?? '';

    // Strip safe public key snippets
    const isSafePublicKey =
      RESULT_SAFE_PREFIXES.some((p) => snip.includes(p)) &&
      !snip.includes('sk_live_') &&
      !snip.includes('STRIPE_SECRET');

    // Strip pure "False Positive" informational patterns — these are knowledge-graph
    // annotations about what NOT to flag, not actual vulnerability detections.
    const isFalsePositiveAnnotation = vc.startsWith('False Positive');

    return !isSafePublicKey && !isFalsePositiveAnnotation;
  });

  // known: true only when at least one result survives safe-list filtering
  const known = filteredRows.length > 0;

  return {
    results: filteredRows.map((row) => ({
      pattern_id: row.pattern_id,
      vuln_class: row.vuln_class,
      stack: row.stack,
      severity: row.severity,
      anonymized_snippet: row.anonymized_snippet,
      fix_snippet: row.fix_snippet,
      post_count: row.post_count,
      confidence: parseFloat(row.similarity?.toFixed(4) ?? 0),
    })),
    known,
    query_info: {
      queries_used_this_month: rateCheck.used,
      monthly_limit: rateCheck.limit,
    },
    auth_hint: authUser.tier === 'anonymous'
      ? 'Sign up free at frogeye.ai to get 50 queries/month and save your history'
      : null,
  };
}

// ─── Tool: frogeye_post ───────────────────────────────────────────────────────

async function toolFrogeyePost({ pattern, language, severity, context }, authUser) {
  // Anonymous users cannot post patterns — API key required
  if (authUser.tier === 'anonymous') {
    return {
      error: 'API key required',
      message: 'Posting patterns requires a Frogeye account. Sign up free at frogeye.ai to contribute to the knowledge graph.',
      auth_hint: 'Sign up free at frogeye.ai to get 50 queries/month and save your history',
    };
  }

  // Validate severity
  const validSeverities = ['low', 'medium', 'high', 'critical'];
  if (!validSeverities.includes(severity)) {
    throw new Error(`Invalid severity "${severity}". Must be one of: ${validSeverities.join(', ')}`);
  }

  // Validate inputs
  if (!pattern || pattern.trim().length < 10) {
    throw new Error('Pattern must be at least 10 characters');
  }

  if (!language || language.trim().length === 0) {
    throw new Error('Language is required');
  }

  // Hash the pattern to detect duplicates
  const patternHash = crypto.createHash('sha256').update(pattern.trim()).digest('hex');

  // Check for duplicate in hitl_queue or patterns
  const dupCheck = await query(
    `SELECT id FROM hitl_queue WHERE pattern_hash = $1 AND user_id = $2
     UNION
     SELECT pattern_id::text FROM patterns WHERE hash = $1
     LIMIT 1`,
    [patternHash, authUser.user_id]
  );

  if (dupCheck.rows.length > 0) {
    return {
      status: 'duplicate',
      message: 'This pattern has already been submitted. Thank you for your contribution.',
      hash: patternHash,
    };
  }

  // Build the pattern draft object
  const patternDraft = {
    raw_pattern: pattern.trim(),
    language: language.trim(),
    severity,
    context: context?.trim() || null,
    submitted_by: authUser.email,
    submitted_at: new Date().toISOString(),
  };

  // Anonymized preview — first 50 chars only (never expose full pattern in response)
  const anonymizedPreview = pattern.trim().slice(0, 50) + (pattern.trim().length > 50 ? '...' : '');

  // Insert into HITL queue — human review required before publishing
  const insertResult = await query(
    `INSERT INTO hitl_queue (user_id, pattern_draft, pattern_hash, status, created_at)
     VALUES ($1, $2, $3, 'pending', NOW())
     RETURNING id, status, created_at`,
    [authUser.user_id, JSON.stringify(patternDraft), patternHash]
  );

  const queued = insertResult.rows[0];

  logger.info('frogeye_post: pattern queued for review', {
    userId: authUser.user_id,
    queueId: queued.id,
    severity,
    language,
  });

  return {
    status: 'queued',
    queue_id: queued.id,
    created_at: queued.created_at,
    anonymized_preview: anonymizedPreview,
    message:
      'Your pattern has been queued for human review. Once approved, it will be published to the knowledge graph and visible to all Frogeye users.',
    severity,
    language,
  };
}

// ─── Tool: frogeye_get_alerts ─────────────────────────────────────────────────

async function toolFrogeyeGetAlerts(args, authUser) {
  // Anonymous users cannot browse alerts — API key required
  if (authUser.tier === 'anonymous') {
    return {
      error: 'API key required',
      message: 'Browsing vulnerability alerts requires a Frogeye account. Sign up free at frogeye.ai.',
      auth_hint: 'Sign up free at frogeye.ai to get 50 queries/month and save your history',
    };
  }

  const rawPage = parseInt(args?.page ?? 0, 10);
  const rawLimit = parseInt(args?.limit ?? 20, 10);
  const page = isNaN(rawPage) || rawPage < 0 ? 0 : rawPage;
  const limit = isNaN(rawLimit) || rawLimit < 1 ? 20 : Math.min(rawLimit, 100);
  const offset = page * limit;

  const WHERE = `WHERE is_public = true AND is_false_positive = false`;

  // Fetch limit+1 rows to detect has_more — avoids a separate COUNT(*) sequential scan.
  // If we get more than `limit` rows, there is a next page; slice back to `limit`.
  const dataResult = await query(
    `SELECT
       pattern_id,
       vuln_class,
       stack,
       severity,
       anonymized_snippet,
       fix_snippet,
       post_count,
       created_at
     FROM patterns
     ${WHERE}
     ORDER BY created_at DESC
     LIMIT $1 OFFSET $2`,
    [limit + 1, offset]
  );

  const has_more = dataResult.rows.length > limit;
  const rows = has_more ? dataResult.rows.slice(0, limit) : dataResult.rows;

  logger.info('frogeye_get_alerts: fetched', {
    userId: authUser.user_id,
    page,
    limit,
    count: rows.length,
    has_more,
  });

  return {
    results: rows.map((row) => ({
      pattern_id: row.pattern_id,
      vuln_class: row.vuln_class,
      stack: row.stack,
      severity: row.severity,
      anonymized_snippet: row.anonymized_snippet,
      fix_snippet: row.fix_snippet,
      post_count: row.post_count,
      created_at: row.created_at,
      confidence: ({ critical: 1.0, high: 0.85, medium: 0.70, low: 0.50 })[row.severity] ?? null,
    })),
    has_more,
    page,
    limit,
  };
}

// ─── Tool: frogeye_register ───────────────────────────────────────────────────

/**
 * Claim a username for this agent. Calls the Frogeye REST API so validation
 * and uniqueness enforcement happen in one place (Next.js route handler).
 * Idempotent: if already registered, returns the current username.
 */
async function toolFrogeyeRegister({ username }, authUser) {
  // Anonymous users cannot register — API key required
  if (authUser.tier === 'anonymous') {
    return {
      error: 'API key required',
      message: 'Registering an agent username requires a Frogeye account. Sign up free at frogeye.ai.',
      auth_hint: 'Sign up free at frogeye.ai to get 50 queries/month and save your history',
    };
  }

  if (!username || typeof username !== 'string' || username.trim() === '') {
    throw new Error('username is required');
  }

  const apiUrl = `${FROGEYE_API_URL}/api/agent/register`;

  let response;
  try {
    response = await fetch(apiUrl, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'x-api-key': authUser.rawApiKey,
      },
      body: JSON.stringify({ username: username.trim() }),
      signal: AbortSignal.timeout(10000),
    });
  } catch (err) {
    logger.error('frogeye_register: fetch failed', { message: err.message });
    throw new Error(`Registration request failed: ${err.message}`);
  }

  const data = await response.json().catch(() => ({ ok: false, error: 'Invalid response from registration API' }));

  if (!response.ok && response.status !== 409) {
    logger.warn('frogeye_register: API error', { status: response.status, data });
  } else if (data.ok) {
    logger.info('frogeye_register: success', {
      userId: authUser.user_id,
      username: data.username,
    });
    // Update cached agent_username so subsequent tools in this session see it
    authUser.agent_username = data.username;
  }

  return data;
}

// ─── Scan record helper ───────────────────────────────────────────────────────

/**
 * Parse a github.com owner/repo from a file or directory path.
 * Handles patterns like:
 *   /tmp/frogeye-scan-abc/github.com/owner/repo/...
 *   /tmp/owner-repo/...  (won't match — safe no-op)
 *   A bare "owner/repo" string
 * Returns { owner, repo } or null if no github.com pattern found.
 */
function parseGithubOwnerRepo(pathStr) {
  if (!pathStr) return null;
  // Match github.com/owner/repo anywhere in the path
  const m = pathStr.match(/github\.com\/([^/]+)\/([^/]+)/);
  if (m) return { owner: m[1], repo: m[2].replace(/\.git$/, '') };
  return null;
}

/**
 * Fire-and-forget: write a scan record to the scans table if the path
 * contains a parseable github.com/owner/repo pattern. Silently no-ops
 * on local paths or errors — never blocks the scan response.
 */
function recordScanResult(pathStr, findingsCount, patternsChecked) {
  const parsed = parseGithubOwnerRepo(pathStr);
  if (!parsed) return; // not a GitHub-originated scan — skip
  const { owner, repo } = parsed;
  query(
    `INSERT INTO scans (repo_owner, repo_name, findings_count, patterns_checked, is_public, scanned_at)
     VALUES ($1, $2, $3, $4, true, NOW())
     ON CONFLICT (repo_owner, repo_name)
     DO UPDATE SET
       scanned_at = NOW(),
       findings_count = EXCLUDED.findings_count,
       patterns_checked = EXCLUDED.patterns_checked`,
    [owner, repo, findingsCount, patternsChecked]
  ).catch((err) => {
    logger.error('recordScanResult: failed to upsert scan record', {
      owner, repo, message: err.message,
    });
  });
}

// ─── Tool: frogeye_scan ───────────────────────────────────────────────────────

// Extensions included in directory scans
const SCAN_EXTENSIONS = new Set(['.js', '.ts', '.jsx', '.tsx', '.py', '.go', '.java', '.rb', '.php', '.swift', '.kt']);

// Directory names skipped entirely during recursive discovery
const SKIP_DIRS = new Set(['node_modules', '.git', 'dist', 'build', '.next', 'coverage', '__pycache__', '.cache', 'vendor']);

// Max files scanned per dir_path call (prevents OOM on very large repos)
const MAX_FILES_PER_DIR_SCAN = 500;

// Extension → language name mapping for auto-detection
const EXT_TO_LANG = {
  '.js': 'javascript', '.ts': 'typescript', '.jsx': 'javascript', '.tsx': 'typescript',
  '.py': 'python', '.go': 'go', '.java': 'java', '.rb': 'ruby',
  '.php': 'php', '.swift': 'swift', '.kt': 'kotlin',
};

/**
 * Recursively collect all source files under dirPath, respecting SKIP_DIRS
 * and SCAN_EXTENSIONS filters. Returns an array of absolute file paths.
 */
function collectSourceFiles(dirPath, results = [], depth = 0) {
  if (depth > 20) return results; // guard against symlink cycles

  let entries;
  try {
    entries = fs.readdirSync(dirPath, { withFileTypes: true });
  } catch (err) {
    return results; // unreadable directory — skip silently
  }

  for (const entry of entries) {
    if (results.length >= MAX_FILES_PER_DIR_SCAN) break;

    const name = entry.name;

    if (entry.isDirectory()) {
      if (SKIP_DIRS.has(name)) continue;
      collectSourceFiles(`${dirPath}/${name}`, results, depth + 1);
    } else if (entry.isFile()) {
      // Skip minified JS and lockfiles
      if (name.endsWith('.min.js') || name.endsWith('.lock')) continue;

      const dotIdx = name.lastIndexOf('.');
      const ext = dotIdx !== -1 ? name.slice(dotIdx) : '';
      if (SCAN_EXTENSIONS.has(ext)) {
        results.push(`${dirPath}/${name}`);
      }
    }
  }

  return results;
}

/**
 * Scan a single content string for vulnerability patterns using pgvector.
 * Returns { findingsMap: Map<pattern_id, finding>, chunksScanned: number, totalLines: number }.
 * findingsMap keys are pattern_ids; values keep the highest-similarity match per pattern.
 */
async function scanContent(sourceContent, language, filePath, authUser) {
  const lines = sourceContent.split('\n');
  const totalLines = lines.length;

  const CHUNK_SIZE = 200;
  const OVERLAP = 20;
  const chunks = [];

  for (let start = 0; start < totalLines; start += CHUNK_SIZE - OVERLAP) {
    const end = Math.min(start + CHUNK_SIZE, totalLines);
    chunks.push({
      text: lines.slice(start, end).join('\n'),
      lineStart: start + 1,
      lineEnd: end,
    });
    if (end === totalLines) break;
  }

  const SIMILARITY_THRESHOLD = 0.75;
  const findingsMap = new Map(); // pattern_id → best finding

  for (const chunk of chunks) {
    // FIX 1: Normalize the chunk before embedding — strips comments, normalizes
    // identifiers, truncates to 4KB. Improves match accuracy vs KB patterns.
    const normalizedChunk = normalizeSnippet(chunk.text);
    const searchText = language ? `language: ${language}\n${normalizedChunk}` : normalizedChunk;

    let embedding;
    try {
      embedding = await generateEmbedding(searchText);
    } catch (err) {
      logger.error('frogeye_scan: embedding failed for chunk', {
        userId: authUser.user_id,
        file: filePath || '<inline>',
        lineStart: chunk.lineStart,
        message: err.message,
      });
      continue;
    }

    const vectorStr = formatVector(embedding);

    let result;
    try {
      result = await query(
        `SELECT
           pattern_id,
           vuln_class,
           severity,
           anonymized_snippet,
           fix_snippet,
           1 - (embedding <=> $1::vector) AS similarity
         FROM patterns
         WHERE is_public = true
           AND embedding IS NOT NULL
           AND is_false_positive = false
           AND 1 - (embedding <=> $1::vector) > $2
         ORDER BY embedding <=> $1::vector
         LIMIT 5`,
        [vectorStr, SIMILARITY_THRESHOLD]
      );
    } catch (err) {
      logger.error('frogeye_scan: DB query failed for chunk', {
        userId: authUser.user_id,
        file: filePath || '<inline>',
        lineStart: chunk.lineStart,
        message: err.message,
      });
      continue;
    }

    for (const row of result.rows) {
      const existing = findingsMap.get(row.pattern_id);
      const similarity = parseFloat(row.similarity?.toFixed(4) ?? 0);

      if (!existing || similarity > existing.similarity) {
        findingsMap.set(row.pattern_id, {
          pattern_id: row.pattern_id,
          vuln_class: row.vuln_class,
          severity: row.severity,
          confidence: similarity,
          line_range: { start: chunk.lineStart, end: chunk.lineEnd },
          snippet_preview: chunk.text.slice(0, 100),
          fix_suggestion: row.fix_snippet || null,
          similarity,
        });
      }
    }
  }

  return { findingsMap, chunksScanned: chunks.length, totalLines };
}

/**
 * Finalize a findings Map into a sorted array (strips internal similarity field).
 * Sort order: critical → high → medium → low, then by similarity desc within tier.
 */
function finalizeFindingsMap(findingsMap) {
  return Array.from(findingsMap.values())
    .sort((a, b) => {
      const order = { critical: 0, high: 1, medium: 2, low: 3 };
      const severityDiff = (order[a.severity] ?? 4) - (order[b.severity] ?? 4);
      return severityDiff !== 0 ? severityDiff : b.similarity - a.similarity;
    })
    .map(({ similarity, ...f }) => f);
}

/**
 * Build a human-readable summary string from a finalized findings array.
 */
function buildScanSummary(findings, scannedDesc) {
  const critical = findings.filter((f) => f.severity === 'critical').length;
  const high = findings.filter((f) => f.severity === 'high').length;
  const medium = findings.filter((f) => f.severity === 'medium').length;
  const low = findings.filter((f) => f.severity === 'low').length;

  const parts = [];
  if (critical > 0) parts.push(`${critical} critical`);
  if (high > 0) parts.push(`${high} high`);
  if (medium > 0) parts.push(`${medium} medium`);
  if (low > 0) parts.push(`${low} low`);

  return findings.length === 0
    ? `No known vulnerability patterns detected in ${scannedDesc}`
    : `Found ${parts.join(', ')} severity issue${findings.length !== 1 ? 's' : ''} in ${scannedDesc}`;
}

/**
 * Scan a file, code snippet, or entire directory for security vulnerabilities
 * using the Frogeye knowledge graph. Splits content into overlapping 200-line
 * chunks, runs pgvector similarity search on each chunk, deduplicates findings
 * by pattern_id, and returns a structured report.
 *
 * Accepts exactly ONE of: file_path, content, or dir_path.
 * dir_path triggers recursive directory scanning — findings grouped by file.
 *
 * Rate-limit policy: the entire scan counts as ONE query (not one per chunk/file).
 * The query counter is incremented once after all processing is complete.
 */
async function toolFrogeyeScan({ file_path, content, dir_path, language, owner, repo }, authUser) {
  // Anonymous users cannot run full scans — API key required
  if (authUser.tier === 'anonymous') {
    return {
      error: 'API key required',
      message: 'Full file scanning requires a Frogeye account. Use frogeye_search for single-snippet queries without an API key. Sign up free at frogeye.ai.',
      auth_hint: 'Sign up free at frogeye.ai to get 50 queries/month and save your history',
    };
  }

  // ── Input validation ────────────────────────────────────────────────────────
  const provided = [file_path, content, dir_path].filter(Boolean).length;
  if (provided === 0) {
    throw new Error('One of file_path, content, or dir_path is required');
  }
  if (provided > 1) {
    throw new Error('Provide exactly one of file_path, content, or dir_path — not multiple');
  }

  // ── Directory scan branch ───────────────────────────────────────────────────
  if (dir_path) {
    // Validate directory exists
    let stat;
    try {
      stat = fs.statSync(dir_path);
    } catch (err) {
      throw new Error(`Cannot access directory "${dir_path}": ${err.message}`);
    }
    if (!stat.isDirectory()) {
      throw new Error(`"${dir_path}" is not a directory — use file_path for individual files`);
    }

    const allFiles = collectSourceFiles(dir_path);
    const truncated = allFiles.length >= MAX_FILES_PER_DIR_SCAN;

    logger.info('frogeye_scan: directory scan starting', {
      userId: authUser.user_id,
      dir: dir_path,
      filesFound: allFiles.length,
      truncated,
    });

    // Scan each file, collecting per-file results
    const fileResults = [];
    let totalFilesWithFindings = 0;
    const globalPatternIds = new Set(); // for unique_patterns count across all files

    for (const fp of allFiles) {
      let fileContent;
      try {
        fileContent = fs.readFileSync(fp, 'utf8');
      } catch (err) {
        logger.error('frogeye_scan: cannot read file', { file: fp, message: err.message });
        continue;
      }

      // Auto-detect language from extension if not explicitly provided
      const ext = fp.slice(fp.lastIndexOf('.'));
      const detectedLang = language || EXT_TO_LANG[ext] || null;

      const { findingsMap, chunksScanned, totalLines } = await scanContent(fileContent, detectedLang, fp, authUser);

      if (findingsMap.size === 0) continue; // skip clean files from output

      const findings = finalizeFindingsMap(findingsMap);
      totalFilesWithFindings++;

      for (const patternId of findingsMap.keys()) {
        globalPatternIds.add(patternId);
      }

      // Make path relative to dir_path for readability
      const relPath = fp.startsWith(dir_path) ? fp.slice(dir_path.length).replace(/^\//, '') : fp;

      fileResults.push({
        file_path: relPath,
        total_lines: totalLines,
        chunks_scanned: chunksScanned,
        findings,
      });
    }

    // Sort: files with most findings first
    fileResults.sort((a, b) => b.findings.length - a.findings.length);

    const totalFindingsCount = fileResults.reduce((sum, f) => sum + f.findings.length, 0);

    // NOTE: queries_used counter is authoritative in Redis (checkRateLimit INCR).
    // DB writes removed to prevent permanent Redis/DB divergence (FRG-044).

    logger.info('frogeye_scan: directory scan complete', {
      userId: authUser.user_id,
      dir: dir_path,
      totalFilesScanned: allFiles.length,
      filesWithFindings: totalFilesWithFindings,
      totalFindings: totalFindingsCount,
      uniquePatterns: globalPatternIds.size,
    });

    const summaryFileDesc = `${allFiles.length} file${allFiles.length !== 1 ? 's' : ''} in ${dir_path}`;
    const summary = totalFindingsCount === 0
      ? `No known vulnerability patterns detected across ${summaryFileDesc}`
      : `Found ${totalFindingsCount} finding${totalFindingsCount !== 1 ? 's' : ''} across ${totalFilesWithFindings} of ${allFiles.length} file${allFiles.length !== 1 ? 's' : ''} scanned`;

    // Fire-and-forget: record scan in scans table if we can parse owner/repo from dir_path
    recordScanResult(dir_path, totalFindingsCount, globalPatternIds.size);

    // Fire-and-forget: explicit owner/repo overrides path-parsed fallback
    if (owner && repo) {
      const _status = totalFindingsCount > 0 ? 'findings' : 'passing';
      const _scannedAt = new Date().toISOString().slice(0, 10);
      const _badgeSecret = process.env.FROGEYE_BADGE_SECRET;
      const _signature = _badgeSecret
        ? crypto.createHmac('sha256', _badgeSecret).update(`${owner}/${repo}:${_scannedAt}`).digest('hex')
        : null;
      ;(async () => {
        try {
          await query(
            `INSERT INTO scans (repo_owner, repo_name, findings_count, patterns_checked, is_public, status, signature, scanned_at)
             VALUES ($1, $2, $3, $4, true, $5, $6, NOW())
             ON CONFLICT (repo_owner, repo_name)
             DO UPDATE SET
               scanned_at = NOW(),
               findings_count = EXCLUDED.findings_count,
               patterns_checked = EXCLUDED.patterns_checked,
               status = EXCLUDED.status,
               signature = EXCLUDED.signature`,
            [owner, repo, totalFindingsCount, globalPatternIds.size, _status, _signature]
          );
        } catch (err) {
          logger.error('frogeye_scan: failed to record explicit scan result', { owner, repo, message: err.message });
        }
      })();
    }

    // Build base directory scan response
    const dirResponse = {
      dir_path,
      summary,
      files_by_findings: fileResults,
      scan_stats: {
        total_files_scanned: allFiles.length,
        files_with_findings: totalFilesWithFindings,
        total_findings: totalFindingsCount,
        unique_patterns: globalPatternIds.size,
        truncated,
        truncated_at: truncated ? MAX_FILES_PER_DIR_SCAN : null,
      },
    };

    // Badge nudges — structured badge object matching single-file branch shape
    const todayStr = new Date().toISOString().slice(0, 10);
    if (owner && repo) {
      const badgeMarkdown = `[![Scanned by Frogeye](https://frogeye.ai/api/badge/passing.svg?date=${todayStr})](https://frogeye.ai/verify/${owner}/${repo})`;

      if (totalFindingsCount === 0) {
        dirResponse.badge = {
          type: 'suggest',
          markdown: badgeMarkdown,
          message: 'Add this badge to your README to show your repo is scanned',
        };
      }

      // Badge staleness nudge — merge into badge object
      try {
        const lastScanRow = await query(
          `SELECT scanned_at FROM scans WHERE repo_owner = $1 AND repo_name = $2 ORDER BY scanned_at DESC LIMIT 1`,
          [owner, repo]
        );
        if (lastScanRow.rows.length > 0) {
          const lastScanDate = new Date(lastScanRow.rows[0].scanned_at);
          const daysSince = Math.floor((Date.now() - lastScanDate.getTime()) / (1000 * 60 * 60 * 24));
          if (daysSince > 90) {
            dirResponse.badge = {
              ...(dirResponse.badge || {}),
              type: 'update',
              markdown: badgeMarkdown,
              message: `Your Frogeye badge is outdated (last scan: ${daysSince} days ago). Update it in your README.`,
              stale: true,
              stale_days: daysSince,
            };
          }
        }
      } catch (badgeErr) {
        logger.error('frogeye_scan: badge staleness check failed (dir)', { owner, repo, message: badgeErr.message });
      }
    } else {
      // No owner/repo — generic badge suggestion
      dirResponse.badge = {
        type: 'suggest',
        markdown: `[![Scanned by Frogeye](https://frogeye.ai/api/badge/passing.svg?date=${todayStr})](https://frogeye.ai)`,
        message: 'Pass owner+repo to frogeye_scan to get a repo-specific verify link',
      };
    }

    return dirResponse;
  }

  // ── Single-file / inline-content branch ────────────────────────────────────
  let sourceContent;
  if (file_path) {
    try {
      sourceContent = fs.readFileSync(file_path, 'utf8');
    } catch (err) {
      throw new Error(`Failed to read file "${file_path}": ${err.message}`);
    }
  } else {
    sourceContent = content;
  }

  logger.info('frogeye_scan: starting scan', {
    userId: authUser.user_id,
    file: file_path || '<inline>',
  });

  const scanStartMs = Date.now();
  const { findingsMap, chunksScanned, totalLines } = await scanContent(sourceContent, language, file_path, authUser);

  // NOTE: queries_used counter is authoritative in Redis (checkRateLimit INCR).
  // DB writes removed to prevent permanent Redis/DB divergence (FRG-044).

  const findings = finalizeFindingsMap(findingsMap);
  const summary = buildScanSummary(findings, `${totalLines} lines`);

  logger.info('frogeye_scan: complete', {
    userId: authUser.user_id,
    totalLines,
    chunksScanned,
    findings: findings.length,
  });

  // Fire-and-forget: record scan in scans table if file_path contains github.com/owner/repo
  if (file_path) recordScanResult(file_path, findings.length, 1);

  // Fire-and-forget: explicit owner/repo overrides path-parsed fallback
  if (owner && repo) {
    const _status = findings.length > 0 ? 'findings' : 'passing';
    const _scannedAt = new Date().toISOString().slice(0, 10);
    const _badgeSecret = process.env.FROGEYE_BADGE_SECRET;
    const _signature = _badgeSecret
      ? crypto.createHmac('sha256', _badgeSecret).update(`${owner}/${repo}:${_scannedAt}`).digest('hex')
      : null;
    ;(async () => {
      try {
        await query(
          `INSERT INTO scans (repo_owner, repo_name, findings_count, patterns_checked, is_public, status, signature, scanned_at)
           VALUES ($1, $2, $3, $4, true, $5, $6, NOW())
           ON CONFLICT (repo_owner, repo_name)
           DO UPDATE SET
             scanned_at = NOW(),
             findings_count = EXCLUDED.findings_count,
             patterns_checked = EXCLUDED.patterns_checked,
             status = EXCLUDED.status,
             signature = EXCLUDED.signature`,
          [owner, repo, findings.length, 1, _status, _signature]
        );
      } catch (err) {
        logger.error('frogeye_scan: failed to record explicit scan result', { owner, repo, message: err.message });
      }
    })();
  }

  const scanEndMs = Date.now();

  // FIX 2: Structured L1ScanResponse shape
  const scanResponse = {
    // Core fields
    file_path: file_path || null,
    language: language || null,
    total_lines: totalLines,
    chunks_scanned: chunksScanned,
    findings,
    findings_count: findings.length,
    known: findings.length > 0,
    scan_ms: scanEndMs - scanStartMs,
    summary,
    // badge and upsell start null — populated below if applicable
    badge: null,
    upsell: null,
  };

  // FIX 3 + badge: Only when owner+repo are provided
  if (owner && repo) {
    const todayStr = new Date().toISOString().slice(0, 10);
    const badgeMarkdown = `[![Scanned by Frogeye](https://frogeye.ai/api/badge/passing.svg?date=${todayStr})](https://frogeye.ai/verify/${owner}/${repo})`;

    if (findings.length > 0) {
      // FIX 2: Badge present on all scan paths — type 'none' when findings exist
      // Dispatch says badge should always be populated, not null, for any scan with owner+repo
      scanResponse.badge = {
        type: 'none',
        markdown: null,
        message: `${findings.length} finding${findings.length !== 1 ? 's' : ''} detected — fix vulnerabilities before adding badge to README`,
      };
    } else {
      // FIX 3: Badge regex check — fetch README and determine type: 'update' vs 'suggest'
      let badgeType = 'suggest';
      try {
        // Try main branch first, fall back to HEAD/master
        let readmeContent = null;
        for (const branch of ['main', 'master', 'HEAD']) {
          const readmeRes = await fetch(
            `https://raw.githubusercontent.com/${owner}/${repo}/${branch}/README.md`,
            { signal: AbortSignal.timeout(5000) }
          );
          if (readmeRes.ok) {
            readmeContent = await readmeRes.text();
            break;
          }
        }
        if (readmeContent && /frogeye\.ai\/api\/badge/.test(readmeContent)) {
          badgeType = 'update'; // badge exists but needs date refresh
        }
      } catch {
        // Silently skip — network failure or private repo, badge type stays 'suggest'
      }

      scanResponse.badge = {
        type: badgeType,
        markdown: badgeMarkdown,
        message: 'Add this badge to your README to show your repo is scanned',
        ...(badgeType === 'update'
          ? { note: 'Update the date in your existing badge to reflect this scan.' }
          : { commit_instruction: 'Add this badge to your README to show your repo is clean.' }),
      };
    }

    // Badge staleness nudge — separate from the clean-scan badge suggestion
    try {
      const lastScanRow = await query(
        `SELECT scanned_at FROM scans WHERE repo_owner = $1 AND repo_name = $2 ORDER BY scanned_at DESC LIMIT 1`,
        [owner, repo]
      );
      if (lastScanRow.rows.length > 0) {
        const lastScanDate = new Date(lastScanRow.rows[0].scanned_at);
        const daysSince = Math.floor((Date.now() - lastScanDate.getTime()) / (1000 * 60 * 60 * 24));
        if (daysSince > 90) {
          // Merge into badge object (or create if null)
          scanResponse.badge = {
            ...(scanResponse.badge || {}),
            stale: true,
            stale_days: daysSince,
            stale_message: `Your Frogeye badge is outdated (last scan: ${daysSince} days ago).`,
            markdown: badgeMarkdown,
          };
        }
      }
    } catch (badgeErr) {
      logger.error('frogeye_scan: badge staleness check failed', { owner, repo, message: badgeErr.message });
    }
  } else {
    // No owner/repo provided — show generic badge suggestion
    const todayStr = new Date().toISOString().slice(0, 10);
    scanResponse.badge = {
      type: 'suggest',
      markdown: `[![Scanned by Frogeye](https://frogeye.ai/api/badge/passing.svg?date=${todayStr})](https://frogeye.ai)`,
      message: 'Pass owner+repo to frogeye_scan to get a repo-specific verify link',
    };
  }

  return scanResponse;
}

// ─── Tool: frogeye_learn ─────────────────────────────────────────────────────

/**
 * Teach Frogeye a team-specific security rule. Writes a private, team-scoped pattern
 * to the KB — visible only when searching with the same API key.
 *
 * Requires auth — anonymous users receive a clear error.
 * is_public = false, is_team_pattern = true so the pattern never leaks to public search.
 */
/**
 * Call HiSolo LLM (Gemini Flash) and collect the full SSE stream response into a string.
 * Returns the text content or throws on network/timeout errors.
 * Fail-open: callers should catch and proceed on error.
 */
async function callLlm(prompt) {
  const response = await fetch(`${HISOLO_PROXY_URL}/api/v1/chat`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'Authorization': `Bearer ${HISOLO_API_TOKEN}`,
    },
    body: JSON.stringify({
      model: 'gemini-2-5-flash',
      messages: [{ role: 'user', content: prompt }],
      max_tokens: 300,
    }),
    signal: AbortSignal.timeout(15000),
  });

  if (!response.ok) {
    const body = await response.text().catch(() => '<unreadable>');
    throw new Error(`LLM API error ${response.status}: ${body}`);
  }

  // Response is SSE — collect all data: chunks and reconstruct text
  const text = await response.text();
  let fullText = '';
  for (const line of text.split('\n')) {
    if (!line.startsWith('data: ')) continue;
    const payload = line.slice(6).trim();
    if (!payload || payload === '[DONE]') continue;
    try {
      const parsed = JSON.parse(payload);
      const part = parsed?.candidates?.[0]?.content?.parts?.[0]?.text ?? '';
      fullText += part;
    } catch (_) {
      // skip unparseable chunks
    }
  }
  return fullText;
}

async function toolFrogeyeLearn({ finding, example, fix, severity }, authUser) {
  // Require authentication — team patterns need a key to scope them
  if (authUser.tier === 'anonymous') {
    return {
      ok: false,
      error: 'auth_required',
      message: 'frogeye_learn requires an API key. Get one at https://frogeye.ai — free tier included.',
      auth_hint: 'Connect with: claude mcp add -t sse -H "x-api-key: fg_live_YOUR_KEY" -s user frogeye https://mcp.frogeye.ai/sse',
    };
  }

  // Free-tier gate — frogeye_learn requires Frog or Apex tier.
  // Free users can search the shared KB but cannot write team-scoped patterns.
  // authUser.tier comes from authenticateApiKey() JOIN on users table — always current.
  if (authUser.tier === 'free') {
    return {
      ok: false,
      error: 'upgrade_required',
      message: "frogeye_learn requires Frogeye Frog ($15/mo). Your findings become your team's institutional memory — patterns you teach are invisible to other teams.",
      upgrade_url: 'https://frogeye.ai/upgrade?ref=mcp-learn',
    };
  }

  // Validate required fields
  if (!finding || typeof finding !== 'string' || finding.trim().length < 5) {
    return { ok: false, error: 'validation_error', message: 'finding is required (min 5 chars)' };
  }
  if (!example || typeof example !== 'string' || example.trim().length < 5) {
    return { ok: false, error: 'validation_error', message: 'example is required (min 5 chars)' };
  }
  const VALID_SEVERITIES = ['critical', 'high', 'medium', 'low'];
  const normalizedSeverity = (severity || 'high').toLowerCase();
  if (!VALID_SEVERITIES.includes(normalizedSeverity)) {
    return { ok: false, error: 'validation_error', message: `severity must be one of: ${VALID_SEVERITIES.join(', ')}` };
  }
  // Reassign so downstream uses normalized value
  severity = normalizedSeverity;

  // ─── STEP A: Gemini Flash quality validation — dual pass, run in parallel (fail-open) ──
  // Pass 1: Is this a real vulnerability?
  // Pass 2 (only when fix is provided): Is the fix safe and free of new issues?
  // Both run concurrently to stay within latency budget.
  let llmResult = null;
  let llmFailed = false;
  let fixSafetyResult = null;  // null when no fix provided or pass 2 failed

  const pass1Prompt = `You are a security vulnerability classifier. Analyze this code snippet and determine if it represents a genuine security vulnerability.\n\nSnippet: ${example.trim().slice(0, 2000)}\nClaimed class: ${finding.trim().slice(0, 200)}\nClaimed severity: ${severity}\n\nReturn JSON only, no markdown: {"is_vulnerability": bool, "corrected_class": string, "confidence": float, "reason": string}`;

  const pass2Prompt = fix && fix.trim().length >= 5
    ? `You are a security code reviewer. A developer has submitted this fix/patch for a ${severity} vulnerability (${finding.trim().slice(0, 200)}).\n\nOriginal vulnerable snippet:\n${example.trim().slice(0, 1000)}\n\nProposed fix:\n${fix.trim().slice(0, 1000)}\n\nDoes this fix resolve the vulnerability without introducing new security issues? Look for: new injection vectors, broken auth, insecure defaults, hardcoded secrets introduced by the patch.\n\nReturn JSON only, no markdown: {"fix_is_safe": bool, "new_issues": [], "confidence": float}`
    : null;

  // Run both passes concurrently
  const [pass1Raw, pass2Raw] = await Promise.allSettled([
    callLlm(pass1Prompt),
    pass2Prompt ? callLlm(pass2Prompt) : Promise.resolve(null),
  ]);

  // Parse Pass 1
  if (pass1Raw.status === 'fulfilled' && pass1Raw.value) {
    try {
      const cleaned = pass1Raw.value.replace(/```json\s*/gi, '').replace(/```\s*/g, '').trim();
      llmResult = JSON.parse(cleaned);
    } catch (parseErr) {
      llmFailed = true;
      logger.error('frogeye_learn: LLM pass 1 parse failed (proceeding)', {
        message: parseErr.message, userId: authUser.user_id,
      });
    }
  } else {
    llmFailed = true;
    logger.error('frogeye_learn: LLM pass 1 failed (proceeding with insertion)', {
      message: pass1Raw.reason?.message ?? 'unknown', userId: authUser.user_id,
    });
  }

  // Parse Pass 2 (only when fix was provided)
  if (pass2Prompt && pass2Raw.status === 'fulfilled' && pass2Raw.value) {
    try {
      const cleaned2 = pass2Raw.value.replace(/```json\s*/gi, '').replace(/```\s*/g, '').trim();
      fixSafetyResult = JSON.parse(cleaned2);
      logger.info('frogeye_learn: LLM pass 2 (fix safety) completed', {
        fix_is_safe: fixSafetyResult.fix_is_safe,
        confidence: fixSafetyResult.confidence,
        new_issues_count: Array.isArray(fixSafetyResult.new_issues) ? fixSafetyResult.new_issues.length : 0,
        userId: authUser.user_id,
      });
    } catch (parseErr2) {
      // Fail-open on pass 2 parse failure — don't block on fix safety if we can't parse
      logger.error('frogeye_learn: LLM pass 2 parse failed (proceeding)', {
        message: parseErr2.message, userId: authUser.user_id,
      });
    }
  } else if (pass2Prompt && pass2Raw.status === 'rejected') {
    logger.error('frogeye_learn: LLM pass 2 failed (proceeding)', {
      message: pass2Raw.reason?.message ?? 'unknown', userId: authUser.user_id,
    });
  }

  // ─── STEP B: Generate embedding for dedup check ───────────────────────────────
  let embedding = null;
  try {
    embedding = await generateEmbedding(finding.trim() + ' ' + example.trim());
  } catch (embedErr) {
    logger.error('frogeye_learn: embedding generation failed pre-insert', {
      message: embedErr.message,
      userId: authUser.user_id,
    });
    // embedding stays null — dedup check skipped, proceed with insertion
  }

  // ─── STEP B: Embedding dedup check (similarity threshold 0.08 = near-identical) ─
  if (embedding) {
    try {
      const vectorStr = formatVector(embedding);
      const dupCheck = await query(
        `SELECT id FROM patterns WHERE embedding IS NOT NULL AND (embedding <=> $1::vector) < 0.08 LIMIT 1`,
        [vectorStr]
      );
      if (dupCheck.rows.length > 0) {
        // Log rejection to audit table (fire-and-forget)
        const snippetHash = crypto.createHash('sha256').update(example.trim()).digest('hex');
        query(
          `INSERT INTO rejections (snippet_hash, claimed_class, llm_verdict, reason)
           VALUES ($1, $2, $3, $4)
           ON CONFLICT DO NOTHING`,
          [snippetHash, finding.trim().slice(0, 200), JSON.stringify(llmResult), 'Similar pattern already exists in the knowledge graph']
        ).catch((err) => {
          logger.error('frogeye_learn: rejections audit write failed', { message: err.message });
        });
        return {
          ok: false,
          status: 'rejected',
          reason: 'Similar pattern already exists in the knowledge graph',
        };
      }
    } catch (dedupErr) {
      // Non-fatal — dedup failure does not block insertion
      logger.error('frogeye_learn: dedup check failed (proceeding)', { message: dedupErr.message });
    }
  }

  // ─── STEP C: Decision logic ───────────────────────────────────────────────────
  // Combined audit payload — always includes both pass results for traceability
  const auditPayload = { pass1: llmResult, pass2: fixSafetyResult };

  if (!llmFailed && llmResult) {
    const isVuln = llmResult.is_vulnerability === true;
    const llmConfidence = typeof llmResult.confidence === 'number' ? llmResult.confidence : 0;

    if (!isVuln || llmConfidence < 0.7) {
      // Reject — log both pass results to audit table (fire-and-forget)
      const snippetHash = crypto.createHash('sha256').update(example.trim()).digest('hex');
      query(
        `INSERT INTO rejections (snippet_hash, claimed_class, llm_verdict, reason)
         VALUES ($1, $2, $3, $4)
         ON CONFLICT DO NOTHING`,
        [snippetHash, finding.trim().slice(0, 200), JSON.stringify(auditPayload), llmResult.reason || 'Snippet does not appear to contain a security vulnerability']
      ).catch((err) => {
        logger.error('frogeye_learn: rejections audit write failed', { message: err.message });
      });
      return {
        ok: false,
        status: 'rejected',
        reason: 'Snippet does not appear to contain a security vulnerability',
        details: llmResult.reason || null,
      };
    }

    // ─── Pass 2 fix safety check — only reject if fix_is_safe=false AND confidence > 0.8 ──
    if (fixSafetyResult !== null) {
      const fixIsSafe = fixSafetyResult.fix_is_safe === true;
      const fixConfidence = typeof fixSafetyResult.confidence === 'number' ? fixSafetyResult.confidence : 0;

      if (!fixIsSafe && fixConfidence > 0.8) {
        const snippetHash = crypto.createHash('sha256').update(example.trim()).digest('hex');
        const newIssues = Array.isArray(fixSafetyResult.new_issues) ? fixSafetyResult.new_issues : [];
        query(
          `INSERT INTO rejections (snippet_hash, claimed_class, llm_verdict, reason)
           VALUES ($1, $2, $3, $4)
           ON CONFLICT DO NOTHING`,
          [snippetHash, finding.trim().slice(0, 200), JSON.stringify(auditPayload), `Fix introduces new security issues: ${newIssues.join('; ')}`]
        ).catch((err) => {
          logger.error('frogeye_learn: rejections audit write failed', { message: err.message });
        });
        return {
          ok: false,
          status: 'rejected',
          reason: 'The proposed fix introduces new security issues',
          new_issues: newIssues,
          fix_safety_confidence: fixConfidence,
        };
      }
    }

    // LLM may have corrected the vuln class — use corrected_class if provided
    if (llmResult.corrected_class && typeof llmResult.corrected_class === 'string' && llmResult.corrected_class.trim()) {
      finding = llmResult.corrected_class.trim();
    }
  }

  // Derive team_id from the API key hash — stable, not reversible
  const teamId = crypto.createHash('sha256').update(authUser.rawApiKey).digest('hex');

  const patternId = crypto.randomUUID();
  // hash is NOT NULL UNIQUE — derive from finding+example to prevent duplicates on re-runs
  const hash = crypto.createHash('sha256').update((finding || '') + ':' + (example || '')).digest('hex');

  // Real confidence: from LLM result (0–1), or null if LLM failed
  const confidence = (!llmFailed && llmResult && typeof llmResult.confidence === 'number')
    ? Math.round(llmResult.confidence * 100) / 100
    : null;

  try {
    const insertResult = await query(
      `INSERT INTO patterns
         (pattern_id, hash, vuln_class, stack, severity, anonymized_snippet, fix_snippet,
          is_public, is_false_positive, is_team_pattern, team_id, post_count)
       VALUES ($1, $2, $3, $4, $5, $6, $7, false, false, true, $8, 1)
       ON CONFLICT (hash) DO NOTHING
       RETURNING pattern_id`,
      [
        patternId,
        hash,
        'team-custom',
        'custom',
        severity,
        example.trim(),
        fix ? fix.trim() : null,
        teamId,
      ]
    );

    // Generate and store embedding if not already generated above
    if (insertResult.rows && insertResult.rows.length > 0) {
      const newPatternId = insertResult.rows[0].pattern_id;
      try {
        const embeddingToStore = embedding || await generateEmbedding(finding.trim() + ' ' + example.trim());
        await query(
          `UPDATE patterns SET embedding = $1 WHERE pattern_id = $2`,
          [formatVector(embeddingToStore), newPatternId]
        );
        logger.info('frogeye_learn: embedding generated and stored', {
          patternId: newPatternId,
          userId: authUser.user_id,
        });
      } catch (embedErr) {
        // Embedding failure is non-fatal — pattern is still saved, search visibility delayed
        logger.error('frogeye_learn: embedding generation failed (pattern still saved)', {
          message: embedErr.message,
          patternId: newPatternId,
        });
      }
    }

    logger.info('frogeye_learn: team pattern written', {
      patternId,
      userId: authUser.user_id,
      severity,
      teamId: teamId.slice(0, 8) + '...',
      confidence,
    });

    const response = {
      ok: true,
      pattern_id: patternId,
      confidence,
      message: `Team pattern saved and embedded. It will appear in frogeye_search results when using your API key.`,
      finding: finding.trim(),
      severity,
      team_scoped: true,
      note: 'This pattern is private to your team. It is never visible to other users.',
    };

    // Include fix safety summary in response if pass 2 ran
    if (fixSafetyResult !== null) {
      response.fix_safety = {
        is_safe: fixSafetyResult.fix_is_safe,
        confidence: typeof fixSafetyResult.confidence === 'number'
          ? Math.round(fixSafetyResult.confidence * 100) / 100
          : null,
        new_issues: Array.isArray(fixSafetyResult.new_issues) ? fixSafetyResult.new_issues : [],
      };
    }

    return response;
  } catch (err) {
    logger.error('frogeye_learn: DB insert failed', { message: err.message, userId: authUser.user_id });
    throw new Error(`Failed to save team pattern: ${err.message}`);
  }
}

// ─── Correlation Rules (hardcoded v1 — no DB table needed) ──────────────────

// ─── Tool: frogeye_correlate ──────────────────────────────────────────────────

/**
 * Correlate multiple vuln_class names to detect compound security risks.
 * Queries correlation_rules table — ILIKE substring matching on pattern_a + pattern_b.
 * No auth required.
 */
async function toolFrogeyeCorrelate({ patterns }) {
  if (!Array.isArray(patterns) || patterns.length === 0) {
    throw new Error('patterns must be a non-empty array of vuln_class strings');
  }

  const inputClasses = patterns.map((p) => `%${String(p).trim()}%`);

  const client = await pool.connect();
  try {
    const result = await client.query(
      `SELECT id, pattern_a, pattern_b, combined_severity, explanation, title
       FROM correlation_rules
       WHERE is_active = true
         AND pattern_a ILIKE ANY($1::text[])
         AND pattern_b ILIKE ANY($1::text[])`,
      [inputClasses]
    );

    const correlations = result.rows.map((row) => ({
      rule_id: row.id,
      elevated_severity: row.combined_severity,
      title: row.title,
      explanation: row.explanation,
      matched_patterns: [row.pattern_a, row.pattern_b],
    }));

    return {
      correlations,
      total_patterns_analyzed: patterns.length,
      compound_risks_found: correlations.length,
      message: correlations.length === 0
        ? 'No compound risks detected. Individual findings are independent.'
        : null,
    };
  } finally {
    client.release();
  }
}

// ─── MCP Server Setup ─────────────────────────────────────────────────────────

const server = new Server(
  {
    name: 'frogeye',
    version: '1.5.17',
  },
  {
    capabilities: {
      tools: {},
    },
  }
);

// Tool registry — JSON Schema for each tool's input
server.setRequestHandler(ListToolsRequestSchema, async () => ({
  tools: [
    {
      name: 'frogeye_search',
      description:
        'Search the Frogeye vulnerability knowledge graph for patterns similar to the given code snippet. Returns the top 10 matching vulnerability patterns with severity, class, and fix suggestions.',
      inputSchema: {
        type: 'object',
        properties: {
          query: {
            type: 'string',
            description: 'The code pattern, snippet, or vulnerability description to search for. Canonical field name (also accepted: pattern).',
          },
          pattern: {
            type: 'string',
            description: 'Alias for query — accepted for backwards compatibility.',
          },
          language: {
            type: 'string',
            description:
              'Programming language or framework (e.g. "javascript", "python", "solidity", "react").',
          },
          context: {
            type: 'string',
            description:
              'Optional additional context about where this pattern appears (e.g. "auth middleware", "payment handler").',
          },
          filename: {
            type: 'string',
            description:
              'Optional filename or path of the file being scanned (e.g. ".env.example", "config/secrets.template"). Used to detect placeholder files that should not be flagged.',
          },
        },
        required: ['language'],
      },
    },
    {
      name: 'frogeye_post',
      description:
        'Submit a new vulnerability pattern to the Frogeye knowledge graph. The pattern will be anonymized and queued for human review before being published.',
      inputSchema: {
        type: 'object',
        properties: {
          pattern: {
            type: 'string',
            description: 'The vulnerable code pattern or description. Minimum 10 characters.',
          },
          language: {
            type: 'string',
            description: 'Programming language or framework where this vulnerability was found.',
          },
          severity: {
            type: 'string',
            enum: ['low', 'medium', 'high', 'critical'],
            description: 'Severity classification of the vulnerability.',
          },
          context: {
            type: 'string',
            description: 'Optional context about the vulnerability (how it was found, impact, etc.).',
          },
        },
        required: ['pattern', 'language', 'severity'],
      },
    },
    {
      name: 'frogeye_register',
      description:
        'Claim a username for this agent. Your patterns will be attributed to this name in the knowledge graph. Idempotent — safe to call multiple times. Call this before posting patterns so your contributions are identified.',
      inputSchema: {
        type: 'object',
        properties: {
          username: {
            type: 'string',
            description:
              'Your agent username (3-32 chars, lowercase alphanumeric and hyphens only, e.g. "claude-security-bot").',
          },
        },
        required: ['username'],
      },
    },
    {
      name: 'frogeye_scan',
      description:
        'Scan a file, code snippet, or entire directory for security vulnerabilities using the Frogeye knowledge graph. Returns a structured report with findings grouped by file, severity, and fix suggestions. Provide exactly one of: file_path (single file), content (inline code), or dir_path (recursive directory scan).',
      inputSchema: {
        type: 'object',
        properties: {
          file_path: {
            type: 'string',
            description: 'Absolute path to the file to scan (e.g. "/app/src/auth.js"). Mutually exclusive with content and dir_path.',
          },
          content: {
            type: 'string',
            description: 'Direct code content to scan. Mutually exclusive with file_path and dir_path.',
          },
          dir_path: {
            type: 'string',
            description: 'Absolute path to a directory to scan recursively (e.g. "/app/src"). Discovers all .js/.ts/.py/.go/.java/.rb/.php/.swift/.kt files, skips node_modules/.git/dist/build/.next/coverage/__pycache__. Returns findings grouped by file with scan_stats summary. Max 500 files per scan. Mutually exclusive with file_path and content.',
          },
          language: {
            type: 'string',
            description: 'Programming language hint (e.g. "python", "javascript", "go"). Optional — auto-detected from file extension when using dir_path.',
          },
          owner: {
            type: 'string',
            description: 'GitHub repo owner (optional) — used to record scan in the Frogeye verify database',
          },
          repo: {
            type: 'string',
            description: 'GitHub repo name (optional) — used to record scan in the Frogeye verify database',
          },
        },
        required: [],
      },
    },
    {
      name: 'frogeye_learn',
      description:
        'Teach Frogeye a team-specific security rule. Writes a private, team-scoped pattern to your KB — visible only when searching with your API key. Requires authentication.',
      inputSchema: {
        type: 'object',
        properties: {
          finding: {
            type: 'string',
            description: 'Description of the vulnerability or security rule (e.g. "All S3 buckets must have public access blocked").',
          },
          example: {
            type: 'string',
            description: 'Code example of the bad pattern or anti-pattern.',
          },
          fix: {
            type: 'string',
            description: 'How to fix or avoid the vulnerability. Optional but strongly recommended.',
          },
          severity: {
            type: 'string',
            enum: ['critical', 'high', 'medium', 'low'],
            description: 'Severity classification of the vulnerability.',
          },
        },
        required: ['finding', 'example', 'severity'],
      },
    },
    {
      name: 'frogeye_correlate',
      description:
        'Correlate multiple vulnerability class names found in a scan to detect compound security risks. Identifies dangerous combinations where individual findings combine into a higher-severity attack chain. No API key required.',
      inputSchema: {
        type: 'object',
        properties: {
          patterns: {
            type: 'array',
            items: { type: 'string' },
            description: 'Array of vuln_class names found in a scan (e.g. ["cors-misconfiguration", "missing-authentication"]). Use the vuln_class values returned by frogeye_search or frogeye_scan.',
          },
        },
        required: ['patterns'],
      },
    },
  ],
}));

// ─── Tool Dispatch (shared between stdio and SSE transports) ──────────────────

/**
 * Handle a tool call. authUser can come from:
 *  - stdio: request.params._meta?.apiKey (set by MCP client config)
 *  - SSE HTTP: extracted from x-api-key header at connection time
 */
server.setRequestHandler(CallToolRequestSchema, async (request, extra) => {
  const { name, arguments: args } = request.params;

  // Check database availability first — return structured error if not configured
  if (!isDatabaseConfigured) {
    logger.warn('Tool call rejected — database not configured', { tool: name });
    return dbNotConfiguredResponse();
  }

  // For SSE transport: auth user is attached to the transport via extra context
  // For stdio transport: API key comes from request meta
  let authUser = extra?.authUser || null;

  if (!authUser) {
    // Stdio fallback: extract from _meta
    const rawApiKey = request.params._meta?.apiKey || null;
    authUser = await authenticateApiKey(rawApiKey);
  }

  if (!authUser) {
    logger.warn('Unauthenticated tool call', { tool: name });
    return {
      content: [
        {
          type: 'text',
          text: JSON.stringify({
            error: 'Unauthorized',
            message:
              'A valid API key is required. Set your Frogeye API key in the MCP client configuration.',
          }),
        },
      ],
      isError: true,
    };
  }

  logger.info('Tool call', { tool: name, userId: authUser.user_id, tier: authUser.tier });

  try {
    let result;

    switch (name) {
      case 'frogeye_search': {
        const raw = await toolFrogeyeSearch(args, authUser);
        const mcpResult = {
          content: [{ type: 'text', text: JSON.stringify(raw, null, 2) }],
        };
        // First-call hook: append registration prompt if not yet registered
        result = withRegistrationPrompt(mcpResult, authUser);
        break;
      }

      case 'frogeye_post': {
        const raw = await toolFrogeyePost(args, authUser);
        const mcpResult = {
          content: [{ type: 'text', text: JSON.stringify(raw, null, 2) }],
        };
        // First-call hook
        result = withRegistrationPrompt(mcpResult, authUser);
        break;
      }

      case 'frogeye_get_alerts': {
        result = {
          content: [{ type: 'text', text: JSON.stringify({ message: 'Community alerts are coming in Phase 2. Use frogeye_search for real-time vulnerability detection.' }) }],
        };
        break;
      }

      case 'frogeye_register': {
        const raw = await toolFrogeyeRegister(args, authUser);
        result = {
          content: [{ type: 'text', text: JSON.stringify(raw, null, 2) }],
        };
        break;
      }

      case 'frogeye_scan': {
        const raw = await toolFrogeyeScan(args, authUser);
        result = {
          content: [{ type: 'text', text: JSON.stringify(raw, null, 2) }],
        };
        break;
      }

      case 'frogeye_learn': {
        const raw = await toolFrogeyeLearn(args, authUser);
        result = {
          content: [{ type: 'text', text: JSON.stringify(raw, null, 2) }],
        };
        break;
      }

      case 'frogeye_correlate': {
        const raw = await toolFrogeyeCorrelate(args);
        result = {
          content: [{ type: 'text', text: JSON.stringify(raw, null, 2) }],
        };
        break;
      }

      default:
        return {
          content: [
            {
              type: 'text',
              text: JSON.stringify({ error: `Unknown tool: ${name}` }),
            },
          ],
          isError: true,
        };
    }

    return result;
  } catch (err) {
    logger.error('Tool execution error', { tool: name, message: err.message, stack: err.stack });
    return {
      content: [
        {
          type: 'text',
          text: JSON.stringify({
            error: 'Tool execution failed',
            message: err.message,
          }),
        },
      ],
      isError: true,
    };
  }
});

// ─── HTTP Server (Cloud Run health checks + SSE transport) ───────────────────

const app = express();

// ─── SSE Transport Session Management ────────────────────────────────────────

/**
 * Map of sessionId → { transport, authUser }
 * Each GET /sse connection gets a unique session ID so POST /message can route correctly.
 */
const sseSessions = new Map();

/**
 * POST /mcp — Stateless StreamableHTTP transport for MCP clients.
 *
 * Supports modern MCP clients that use the StreamableHTTP protocol instead of
 * the legacy SSE transport. Each request is stateless — no session is maintained.
 *
 * Auth: x-api-key header (same key format as SSE mode).
 * On success: returns JSON-RPC response. On failure: 401.
 */
// FIX 1: Normalize Accept header for POST /mcp.
// The MCP SDK StreamableHTTPServerTransport.handleRequest() requires text/event-stream in
// the Accept header even for JSON-RPC tool calls (not just SSE streams). Clients that send
// only "application/json" get a 406 Not Acceptable. We inject text/event-stream as a
// synchronous Express middleware BEFORE validateApiKey so the SDK sees it in req.headers.
// Using Object.assign with a fresh object to ensure the mutation is visible to the SDK
// regardless of how the SDK reads req.headers (getter vs property access).
function normalizeMcpAcceptHeader(req, _res, next) {
  // SDK checks req.headers.accept for "text/event-stream" before processing POST /mcp.
  // Clients that only send application/json get a 406. Mutate in-place on the existing
  // req.headers object (a plain IncomingMessage headers dict — safe to mutate).
  const accept = req.headers['accept'] || '';
  if (!accept.includes('text/event-stream')) {
    req.headers['accept'] = accept ? accept + ', text/event-stream' : 'application/json, text/event-stream';
  }
  next();
}

app.post('/mcp', express.json(), normalizeMcpAcceptHeader, validateApiKey, async (req, res) => {
  const authUser = req.authUser;
  try {
    const transport = new StreamableHTTPServerTransport({
      sessionIdGenerator: undefined, // stateless — no session tracking
    });
    const boundServer = createBoundServer(authUser, null);
    await boundServer.connect(transport);
    await transport.handleRequest(req, res, req.body);
    res.on('close', () => {
      transport.close();
      boundServer.close();
    });
  } catch (err) {
    process.stderr.write(`[mcp] POST /mcp error: ${err.message}\n`);
    if (!res.headersSent) {
      res.status(500).json({
        jsonrpc: '2.0',
        error: { code: -32603, message: 'Internal server error' },
        id: null,
      });
    }
  }
});

/**
 * GET /sse — Establish SSE connection for MCP HTTP transport.
 *
 * Claude CLI connects here with:
 *   claude mcp add -t http -H 'x-api-key: YOUR_KEY' -s user frogeye https://mcp.frogeye.ai/sse
 *
 * Auth: x-api-key header (same key format as stdio mode).
 * On success: streams SSE events. On failure: 401.
 */
app.get('/sse', validateApiKey, async (req, res) => {
  // authUser resolved and attached by validateApiKey middleware
  const authUser = req.authUser;

  // Set CORS headers BEFORE handing off to SSEServerTransport.
  // The SDK's start() calls res.writeHead(200, ...) internally — do NOT call
  // res.setHeader/flushHeaders here or we get ERR_HTTP_HEADERS_SENT.
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Headers', 'x-api-key, authorization, content-type');

  // Create SSE transport. The SDK sends:
  //   event: endpoint
  //   data: /message?sessionId=<uuid>
  // automatically on connect(). The sessionId is transport._sessionId (set in constructor).
  const transport = new SSEServerTransport('/message', res);

  // CRITICAL: Register session BEFORE calling connect() / start().
  // connect() calls transport.start() which sends the endpoint event over SSE.
  // If the client is fast, it will immediately POST to /message?sessionId=...
  // before sseSessions.set() is called — causing "No active SSE connection" 503.
  // Fix: register the transport in sseSessions FIRST, then let connect() send the event.
  const sessionId = transport._sessionId;
  sseSessions.set(sessionId, { transport, authUser });

  // Connect MCP server to transport — sends the endpoint event to the client.
  // Pass res so the bound server can emit keepalive pings during long tool calls
  // (embedding + pgvector can take 600ms+ — enough for Cloud Run GFE to drop the stream).
  const boundServer = createBoundServer(authUser, res);
  await boundServer.connect(transport);

  logger.info('SSE session ready', { sessionId, userId: authUser.user_id, tier: authUser.tier });

  // Keepalive: send SSE comment lines every 25s to prevent Cloud Run idle timeout (default: 60s)
  // Without this, the connection is dropped silently after 2-3 queries with a 503.
  const keepalive = setInterval(() => {
    if (!res.writableEnded) {
      res.write(': keepalive\n\n');
    } else {
      clearInterval(keepalive);
    }
  }, 25000);

  // Clean up on disconnect
  req.on('close', () => {
    clearInterval(keepalive);
    logger.info('SSE connection closed', { sessionId, userId: authUser.user_id });
    sseSessions.delete(sessionId);
    boundServer.close().catch(() => {});
  });
});

/**
 * POST /message — Receive JSON-RPC messages from SSE clients.
 *
 * The MCP SDK's SSEServerTransport expects POST messages routed to the transport
 * instance that owns the SSE stream. We use the sessionId query param for routing.
 */
app.post('/message', express.json(), async (req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Headers', 'x-api-key, authorization, content-type');

  // Route to the correct SSE session
  const sessionId = req.query.sessionId;
  const session = sessionId ? sseSessions.get(sessionId) : null;

  // If no sessionId, try the most recent session (single-client convenience)
  const { transport } = session || ([...sseSessions.values()].at(-1) ?? {});

  if (!transport) {
    res.status(503).json({ error: 'No active SSE connection. Connect to /sse first.' });
    return;
  }

  try {
    // Pass req.body as parsedBody — express.json() already consumed the stream,
    // so raw-body inside handlePostMessage would get an empty buffer otherwise.
    await transport.handlePostMessage(req, res, req.body);
  } catch (err) {
    logger.error('POST /message error', { message: err.message });
    if (!res.headersSent) {
      res.status(500).json({ error: 'Message handling failed' });
    }
  }
});

/**
 * OPTIONS preflight for CORS (needed for browser clients and some MCP hosts)
 */
app.options('/sse', (req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'x-api-key, authorization, content-type');
  res.status(204).send();
});

app.options('/message', (req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'x-api-key, authorization, content-type');
  res.status(204).send();
});

// Health check — Cloud Run readiness/liveness probe
// Registered for both /healthz and /healthz/ — Cloud Run intercepts the no-slash variant at
// the load balancer level, but Vercel proxy may strip the trailing slash, so both are needed.
app.get(['/healthz', '/healthz/'], async (_req, res) => {
  if (!isDatabaseConfigured) {
    // Service is alive but database not yet configured — return 200 so Cloud Run
    // doesn't kill the container while waiting for Neon to be provisioned.
    return res.json({
      status: 'degraded',
      version: '1.5.17',
      service: 'frogeye-mcp',
      reason: 'DATABASE_URL not configured — set it in Cloud Run environment',
    });
  }

  try {
    // Verify DB connectivity
    await query('SELECT 1', []);
    res.json({ status: 'ok', version: '1.5.17', service: 'frogeye-mcp' });
  } catch (err) {
    logger.error('/healthz: DB check failed', { message: err.message });
    res.status(503).json({ status: 'error', reason: 'database unavailable' });
  }
});

// Liveness probe (no DB check — just process is alive)
app.get('/live', (_req, res) => {
  res.status(200).send('alive');
});

// Root route — machine-readable discovery document
app.get('/', (_req, res) => {
  res.status(200).json({
    name: 'frogeye',
    version: '1.5.17',
    transport: 'sse',
    endpoint: '/sse',
    anonymous_access: true,
    docs: 'https://frogeye.ai',
    health: '/live',
  });
});

// Catch-all for unexpected HTTP requests
app.use((req, res) => {
  res.status(404).json({
    error: 'Not found',
    hint: "Frogeye MCP server. Connect via: claude mcp add -t sse -H 'x-api-key: YOUR_KEY' -s user frogeye https://mcp.frogeye.ai/sse",
  });
});

// ─── Per-Connection MCP Server Factory ───────────────────────────────────────

/**
 * Create a new MCP Server instance bound to a specific authUser.
 * This allows each SSE connection to have its own authenticated context
 * without sharing state between connections.
 */
function createBoundServer(authUser, sseRes) {
  const boundServer = new Server(
    { name: 'frogeye', version: '1.5.17' },
    { capabilities: { tools: {} } }
  );

  boundServer.setRequestHandler(ListToolsRequestSchema, async () => ({
    tools: [
      {
        name: 'frogeye_search',
        description:
          'Search the Frogeye vulnerability knowledge graph for patterns similar to the given code snippet. Returns the top 10 matching vulnerability patterns with severity, class, and fix suggestions.',
        inputSchema: {
          type: 'object',
          properties: {
            query: { type: 'string', description: 'The code pattern, snippet, or vulnerability description to search for. Canonical field name (also accepted: pattern).' },
            pattern: { type: 'string', description: 'Alias for query — accepted for backwards compatibility.' },
            language: { type: 'string', description: 'Programming language or framework (e.g. "javascript", "python", "solidity", "react").' },
            context: { type: 'string', description: 'Optional additional context about where this pattern appears (e.g. "auth middleware", "payment handler").' },
            filename: { type: 'string', description: 'Optional filename or path of the file being scanned (e.g. ".env.example", "config/secrets.template"). Used to detect placeholder files that should not be flagged.' },
          },
          required: ['language'],
        },
      },
      {
        name: 'frogeye_post',
        description:
          'Submit a new vulnerability pattern to the Frogeye knowledge graph. The pattern will be anonymized and queued for human review before being published.',
        inputSchema: {
          type: 'object',
          properties: {
            pattern: { type: 'string', description: 'The vulnerable code pattern or description. Minimum 10 characters.' },
            language: { type: 'string', description: 'Programming language or framework where this vulnerability was found.' },
            severity: { type: 'string', enum: ['low', 'medium', 'high', 'critical'], description: 'Severity classification of the vulnerability.' },
            context: { type: 'string', description: 'Optional context about the vulnerability (how it was found, impact, etc.).' },
          },
          required: ['pattern', 'language', 'severity'],
        },
      },
      {
        name: 'frogeye_register',
        description: 'Claim a username for this agent. Idempotent — safe to call multiple times.',
        inputSchema: {
          type: 'object',
          properties: {
            username: { type: 'string', description: 'Your agent username (3-32 chars, lowercase alphanumeric and hyphens only).' },
          },
          required: ['username'],
        },
      },
      {
        name: 'frogeye_scan',
        description: 'Scan a file, code snippet, or entire directory for security vulnerabilities using the Frogeye knowledge graph. Returns findings grouped by file with scan_stats summary. Provide exactly one of: file_path, content, or dir_path.',
        inputSchema: {
          type: 'object',
          properties: {
            file_path: { type: 'string', description: 'Absolute path to the file to scan. Mutually exclusive with content and dir_path.' },
            content: { type: 'string', description: 'Direct code content to scan. Mutually exclusive with file_path and dir_path.' },
            dir_path: { type: 'string', description: 'Absolute path to a directory to scan recursively. Skips node_modules/.git/dist/build/.next/coverage. Returns findings grouped by file. Max 500 files.' },
            language: { type: 'string', description: 'Programming language hint. Auto-detected from extension in dir_path mode.' },
            owner: { type: 'string', description: 'GitHub repo owner (optional) — used to record scan in the Frogeye verify database' },
            repo: { type: 'string', description: 'GitHub repo name (optional) — used to record scan in the Frogeye verify database' },
          },
          required: [],
        },
      },
      {
        name: 'frogeye_learn',
        description: 'Teach Frogeye a team-specific security rule. Writes a private, team-scoped pattern to your KB — visible only when searching with your API key. Requires authentication.',
        inputSchema: {
          type: 'object',
          properties: {
            finding: { type: 'string', description: 'Description of the vulnerability or security rule.' },
            example: { type: 'string', description: 'Code example of the bad pattern.' },
            fix: { type: 'string', description: 'How to fix or avoid the vulnerability. Optional but recommended.' },
            severity: { type: 'string', enum: ['critical', 'high', 'medium', 'low'], description: 'Severity classification.' },
          },
          required: ['finding', 'example', 'severity'],
        },
      },
      {
        name: 'frogeye_correlate',
        description: 'Correlate multiple vulnerability class names found in a scan to detect compound security risks. Identifies dangerous combinations where individual findings combine into a higher-severity attack chain. No API key required.',
        inputSchema: {
          type: 'object',
          properties: {
            patterns: {
              type: 'array',
              items: { type: 'string' },
              description: 'Array of vuln_class names found in a scan (e.g. ["cors-misconfiguration", "missing-authentication"]). Use the vuln_class values returned by frogeye_search or frogeye_scan.',
            },
          },
          required: ['patterns'],
        },
      },
    ],
  }));

  boundServer.setRequestHandler(CallToolRequestSchema, async (request) => {
    const { name, arguments: args } = request.params;

    if (!isDatabaseConfigured) {
      logger.warn('Tool call rejected — database not configured', { tool: name });
      return dbNotConfiguredResponse();
    }

    logger.info('SSE tool call', { tool: name, userId: authUser.user_id, tier: authUser.tier });

    // P0 SSE FIX: Emit keepalive SSE comments every 500ms during tool execution.
    // Embedding + pgvector queries take 600ms+ — Cloud Run GFE drops idle SSE streams
    // before the response arrives, causing "frogeye_search aborting mid-call" errors.
    // SSE comment lines (": keepalive\n\n") are invisible to MCP clients but prevent
    // the GFE from treating the stream as idle.
    const keepaliveInterval = sseRes
      ? setInterval(() => {
          if (!sseRes.writableEnded) {
            sseRes.write(': keepalive\n\n');
          }
        }, 500)
      : null;

    try {
      let result;
      switch (name) {
        case 'frogeye_search': {
          const raw = await toolFrogeyeSearch(args, authUser);
          const mcpResult = { content: [{ type: 'text', text: JSON.stringify(raw, null, 2) }] };
          result = withRegistrationPrompt(mcpResult, authUser);
          break;
        }
        case 'frogeye_post': {
          const raw = await toolFrogeyePost(args, authUser);
          const mcpResult = { content: [{ type: 'text', text: JSON.stringify(raw, null, 2) }] };
          result = withRegistrationPrompt(mcpResult, authUser);
          break;
        }
        case 'frogeye_get_alerts': {
          result = { content: [{ type: 'text', text: JSON.stringify({ message: 'Community alerts are coming in Phase 2. Use frogeye_search for real-time vulnerability detection.' }) }] };
          break;
        }
        case 'frogeye_register': {
          const raw = await toolFrogeyeRegister(args, authUser);
          result = { content: [{ type: 'text', text: JSON.stringify(raw, null, 2) }] };
          break;
        }
        case 'frogeye_scan': {
          const raw = await toolFrogeyeScan(args, authUser);
          result = { content: [{ type: 'text', text: JSON.stringify(raw, null, 2) }] };
          break;
        }
        case 'frogeye_learn': {
          const raw = await toolFrogeyeLearn(args, authUser);
          result = { content: [{ type: 'text', text: JSON.stringify(raw, null, 2) }] };
          break;
        }
        case 'frogeye_correlate': {
          const raw = await toolFrogeyeCorrelate(args);
          result = { content: [{ type: 'text', text: JSON.stringify(raw, null, 2) }] };
          break;
        }
        default:
          if (keepaliveInterval) clearInterval(keepaliveInterval);
          return {
            content: [{ type: 'text', text: JSON.stringify({ error: `Unknown tool: ${name}` }) }],
            isError: true,
          };
      }
      if (keepaliveInterval) clearInterval(keepaliveInterval);
      return result;
    } catch (err) {
      if (keepaliveInterval) clearInterval(keepaliveInterval);
      logger.error('SSE tool execution error', { tool: name, message: err.message });
      return {
        content: [{ type: 'text', text: JSON.stringify({ error: 'Tool execution failed', message: err.message }) }],
        isError: true,
      };
    }
  });

  return boundServer;
}

// ─── Startup ──────────────────────────────────────────────────────────────────

async function main() {
  logger.info('Frogeye MCP Server starting', { version: '1.5.17', env: NODE_ENV });

  // Start HTTP server FIRST (Cloud Run listens on $PORT immediately)
  // Must bind before DB check — Cloud Run health probe fires immediately
  const httpServer = app.listen(PORT, () => {
    logger.info(`HTTP server listening on port ${PORT} (SSE: GET /sse, POST /message, health: GET /healthz)`);
  });

  httpServer.on('error', (err) => {
    logger.error('HTTP server error', { message: err.message });
    process.exit(1);
  });

  // Verify DB is reachable (non-fatal on startup if DATABASE_URL not yet set)
  if (isDatabaseConfigured) {
    try {
      await query('SELECT 1', []);
      logger.info('Database connection verified');
    } catch (err) {
      logger.error('Database connection failed on startup — continuing in degraded mode', { message: err.message });
      // Don't exit — Cloud Run health check is on /healthz (Express), not DB
      // Tools will return errors until DB is populated, but the service stays alive
    }

    // FIX v1.5.17: Create rejections audit table — idempotent, used by frogeye_learn quality pipeline.
    try {
      await query(`
        CREATE TABLE IF NOT EXISTS rejections (
          id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
          snippet_hash TEXT NOT NULL,
          claimed_class TEXT,
          llm_verdict JSONB,
          reason TEXT,
          created_at TIMESTAMPTZ DEFAULT NOW()
        )
      `, []);
      await query(`CREATE INDEX IF NOT EXISTS idx_rejections_hash ON rejections(snippet_hash)`, []);
      await query(`CREATE INDEX IF NOT EXISTS idx_rejections_created ON rejections(created_at DESC)`, []);
      logger.info('rejections table ensured');
    } catch (rejErr) {
      logger.warn('rejections table creation skipped', { message: rejErr.message });
    }

    // FIX 4: scans table DDL hardening — idempotent constraint + index creation.
    // Uses PL/pgSQL exception handling because PostgreSQL does NOT support
    // "ADD CONSTRAINT IF NOT EXISTS" — duplicate_object is the correct catch.
    try {
      await query(`
        DO $
        BEGIN
          ALTER TABLE scans ADD CONSTRAINT chk_scans_status
            CHECK (status IN ('pass', 'fail', 'error', 'passing', 'findings'));
        EXCEPTION WHEN duplicate_object THEN NULL;
        END
        $;
      `, []);

      await query(`CREATE INDEX IF NOT EXISTS idx_scans_repo ON scans(repo_owner, repo_name)`, []);
      await query(`CREATE INDEX IF NOT EXISTS idx_scans_date ON scans(scanned_at DESC)`, []);
      await query(`CREATE INDEX IF NOT EXISTS idx_scans_status ON scans(status)`, []);

      logger.info('scans table DDL hardening: CHECK constraint + 3 indexes ensured');
    } catch (ddlErr) {
      // Non-fatal — if scans table doesn't exist yet, skip silently
      logger.warn('scans DDL hardening skipped (table may not exist yet)', { message: ddlErr.message });
    }
  } else {
    logger.warn('Skipping DB connectivity check — DATABASE_URL not set (degraded mode)');
  }

  // Start MCP server over stdio only if stdin is a pipe (Claude Code connecting via stdio)
  // In Cloud Run HTTP mode, stdin is not a pipe — skip stdio transport
  const isStdioPipe = !process.stdin.isTTY && process.env.MCP_TRANSPORT !== 'http';
  if (isStdioPipe) {
    const transport = new StdioServerTransport();
    await server.connect(transport);
    logger.info('MCP server connected via StdioServerTransport');
  } else {
    logger.info('Running in HTTP-only mode (no stdin pipe) — connect via GET /sse');
  }

  // ─── Graceful Shutdown ─────────────────────────────────────────────────────
  let shuttingDown = false;

  async function shutdown(signal) {
    if (shuttingDown) return;
    shuttingDown = true;

    logger.info(`Received ${signal} — shutting down gracefully`);

    // Close all SSE sessions
    for (const [sessionId, { transport }] of sseSessions) {
      try {
        await transport.close?.();
      } catch {}
      sseSessions.delete(sessionId);
    }

    // Stop accepting new HTTP connections
    httpServer.close(() => {
      logger.info('HTTP server closed');
    });

    // Drain DB pool (only if configured)
    if (pool) {
      try {
        await pool.end();
        logger.info('DB pool drained');
      } catch (err) {
        logger.error('DB pool drain error', { message: err.message });
      }
    }

    // Close MCP server
    try {
      await server.close();
      logger.info('MCP server closed');
    } catch (err) {
      logger.error('MCP server close error', { message: err.message });
    }

    logger.info('Shutdown complete');
    process.exit(0);
  }

  process.on('SIGTERM', () => shutdown('SIGTERM'));
  process.on('SIGINT', () => shutdown('SIGINT'));

  // Unhandled rejection guard — log and don't crash silently
  process.on('unhandledRejection', (reason, promise) => {
    logger.error('Unhandled promise rejection', {
      reason: String(reason),
      promise: String(promise),
    });
  });

  process.on('uncaughtException', (err) => {
    logger.error('Uncaught exception — exiting', { message: err.message, stack: err.stack });
    process.exit(1);
  });
}

main().catch((err) => {
  process.stderr.write(JSON.stringify({ level: 'error', msg: 'Fatal startup error', error: err.message }) + '\n');
  process.exit(1);
});
