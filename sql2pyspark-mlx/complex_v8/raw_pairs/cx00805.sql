WITH recent AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        csat,
        tags
    FROM tickets
    WHERE requester_id >= 8
),
with_prev AS (
    -- Previous opened_at per priority
    SELECT *, LAG(opened_at) OVER (PARTITION BY priority ORDER BY opened_at, id) AS prev_opened_at
    FROM recent
),
flagged AS (
    -- Flag rows that start a new session (gap > 2 hours)
    SELECT *, CASE WHEN prev_opened_at IS NULL OR opened_at > prev_opened_at + INTERVAL 2 HOURS THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY priority ORDER BY opened_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
per_session AS (
    -- One row per session
    SELECT
        priority,
        session_id,
        MIN(opened_at) AS session_start,
        MAX(opened_at) AS session_end,
        COUNT(*) AS events,
        SUM(csat) AS session_csat,
        ROUND((UNIX_TIMESTAMP(MAX(opened_at)) - UNIX_TIMESTAMP(MIN(opened_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY priority, session_id
),
without_matches AS (
    -- Compare against the overall avg
    SELECT *
    FROM per_session
    WHERE session_id > (SELECT AVG(session_id) FROM per_session)
),
annotated AS (
    -- Add calculated columns
    SELECT
        *,
        COALESCE(duration_min, 0.0) AS duration_min_filled,
        DATE_TRUNC('DAY', session_start) AS session_start_day
    FROM without_matches
    WHERE session_csat <= 13
)
-- Final projection
SELECT
    priority,
    session_start,
    session_end,
    events,
    session_csat,
    duration_min,
    duration_min_filled,
    session_start_day,
    SUBSTRING(TRIM(priority), 1, 4) AS priority_prefix,
    LOWER(TRIM(priority)) AS priority_clean
FROM annotated
ORDER BY session_start_day DESC, session_end
