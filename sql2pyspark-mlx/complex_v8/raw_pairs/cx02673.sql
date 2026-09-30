WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        csat,
        tags
    FROM tickets
),
with_prev AS (
    -- Previous opened_at per priority
    SELECT *, LAG(opened_at) OVER (PARTITION BY priority ORDER BY opened_at, id) AS prev_opened_at
    FROM base
),
flagged AS (
    -- Flag rows that start a new session (gap > 45 minutes)
    SELECT *, CASE WHEN prev_opened_at IS NULL OR opened_at > prev_opened_at + INTERVAL 45 MINUTES THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY priority ORDER BY opened_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
session_summary AS (
    -- One row per session
    SELECT
        priority,
        burst_id,
        MIN(opened_at) AS session_start,
        MAX(opened_at) AS session_end,
        COUNT(*) AS n_events,
        SUM(csat) AS session_csat,
        ROUND((UNIX_TIMESTAMP(MAX(opened_at)) - UNIX_TIMESTAMP(MIN(opened_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY priority, burst_id
),
derived AS (
    -- Derive helper columns
    SELECT
        priority,
        burst_id,
        session_start,
        n_events,
        session_csat,
        duration_min,
        LOWER(REGEXP_REPLACE(TRIM(priority), ' ', '_')) AS priority_slug,
        DATE_FORMAT(session_start, 'MMM yyyy') AS session_start_label,
        SUBSTRING(TRIM(priority), 1, 3) AS priority_prefix
    FROM session_summary
    WHERE priority IN ('P1', 'P3', 'P2')
),
enriched AS (
    -- Derive helper columns
    SELECT
        *,
        ROUND(session_csat / NULLIF(duration_min, 0), 3) AS session_csat_per_duration_min
    FROM derived
)
-- Final projection
SELECT
    priority,
    burst_id,
    session_start,
    duration_min,
    priority_slug,
    session_start_label,
    priority_prefix,
    session_csat_per_duration_min,
    FLOOR(session_csat / 5) * 5 AS session_csat_bucket,
    ROUND(duration_min * 1.1, 2) AS duration_min_adj
FROM enriched
ORDER BY session_start DESC, priority
