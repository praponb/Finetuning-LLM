WITH scoped AS (
    -- Scope the tickets rows
    SELECT
        id,
        agent_id,
        opened_at,
        closed_at,
        tags,
        INITCAP(TRIM(channel)) AS channel_clean
    FROM tickets
    WHERE TO_DATE(opened_at) < DATE '2025-02-12'
),
with_prev AS (
    -- Previous opened_at per agent_id
    SELECT *, LAG(opened_at) OVER (PARTITION BY agent_id ORDER BY opened_at, id) AS prev_opened_at
    FROM scoped
),
flagged AS (
    -- Flag rows that start a new session (gap > 45 minutes)
    SELECT *, CASE WHEN prev_opened_at IS NULL OR opened_at > prev_opened_at + INTERVAL 45 MINUTES THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY agent_id ORDER BY opened_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
sessions AS (
    -- One row per session
    SELECT
        agent_id,
        burst_id,
        MIN(opened_at) AS session_start,
        MAX(opened_at) AS session_end,
        COUNT(*) AS n_events,
        ROUND((UNIX_TIMESTAMP(MAX(opened_at)) - UNIX_TIMESTAMP(MIN(opened_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY agent_id, burst_id
),
rollup_base AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_TRUNC('MONTH', session_end) AS session_end_month,
        n_events,
        ROUND(SUM(duration_min), 2) AS total_duration_min,
        PERCENTILE_APPROX(burst_id, 0.5) AS median_burst_id,
        MAX(session_start) AS last_session_start,
        SUM(n_events) AS total_n_events,
        PERCENTILE_APPROX(duration_min, 0.5) AS median_duration_min
    FROM sessions
    GROUP BY DATE_TRUNC('MONTH', session_end), n_events
)
-- Final projection
SELECT
    session_end_month,
    n_events,
    median_burst_id,
    last_session_start,
    total_n_events,
    median_duration_min,
    CASE WHEN median_burst_id >= 5 THEN 'HIGH' WHEN median_burst_id >= 3 THEN 'MEDIUM' ELSE 'LOW' END AS median_burst_id_tier
FROM rollup_base
ORDER BY session_end_month NULLS LAST, total_n_events
