WITH cleaned AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        priority,
        channel
    FROM tickets
),
with_prev AS (
    -- Previous closed_at per agent_id
    SELECT *, LAG(closed_at) OVER (PARTITION BY agent_id ORDER BY closed_at, id) AS prev_closed_at
    FROM cleaned
),
flagged AS (
    -- Flag rows that start a new session (gap > 45 minutes)
    SELECT *, CASE WHEN prev_closed_at IS NULL OR closed_at > prev_closed_at + INTERVAL 45 MINUTES THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY agent_id ORDER BY closed_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
session_summary AS (
    -- One row per session
    SELECT
        agent_id,
        session_id,
        MIN(closed_at) AS session_start,
        MAX(closed_at) AS session_end,
        COUNT(*) AS events,
        SUM(requester_id) AS session_requester_id,
        ROUND((UNIX_TIMESTAMP(MAX(closed_at)) - UNIX_TIMESTAMP(MIN(closed_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY agent_id, session_id
)
-- Final projection
SELECT
    agent_id,
    session_id,
    session_end,
    events,
    session_requester_id,
    duration_min
FROM session_summary
ORDER BY session_end DESC, duration_min DESC
