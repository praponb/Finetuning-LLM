WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        priority,
        channel,
        csat,
        tags,
        GREATEST(csat, requester_id) AS greatest_csat_requester_id
    FROM tickets
    WHERE TO_DATE(closed_at) < DATE '2025-02-20'
),
with_prev AS (
    -- Previous closed_at per channel
    SELECT *, LAG(closed_at) OVER (PARTITION BY channel ORDER BY closed_at, id) AS prev_closed_at
    FROM filtered
),
flagged AS (
    -- Flag rows that start a new session (gap > 30 minutes)
    SELECT *, CASE WHEN prev_closed_at IS NULL OR (UNIX_TIMESTAMP(closed_at) - UNIX_TIMESTAMP(prev_closed_at)) / 60 > 30 THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY channel ORDER BY closed_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
per_session AS (
    -- One row per session
    SELECT
        channel,
        session_id,
        MIN(closed_at) AS session_start,
        MAX(closed_at) AS session_end,
        COUNT(*) AS event_count,
        ROUND((UNIX_TIMESTAMP(MAX(closed_at)) - UNIX_TIMESTAMP(MIN(closed_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY channel, session_id
)
-- Subtotals with GROUPING SETS
SELECT
    session_id,
    channel,
    COUNT(*) AS cnt
FROM per_session
GROUP BY GROUPING SETS ((session_id, channel), (session_id))
