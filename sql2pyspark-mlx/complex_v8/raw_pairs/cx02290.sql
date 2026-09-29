WITH recent_tickets AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        tags
    FROM tickets
),
with_prev AS (
    -- Previous opened_at per agent_id
    SELECT *, LAG(opened_at) OVER (PARTITION BY agent_id ORDER BY opened_at, id) AS prev_opened_at
    FROM recent_tickets
),
flagged AS (
    -- Flag rows that start a new session (gap > 2 hours)
    SELECT *, CASE WHEN prev_opened_at IS NULL OR opened_at > prev_opened_at + INTERVAL 2 HOURS THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY agent_id ORDER BY opened_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
per_session AS (
    -- One row per session
    SELECT
        agent_id,
        session_no,
        MIN(opened_at) AS session_start,
        MAX(opened_at) AS session_end,
        COUNT(*) AS event_count,
        SUM(requester_id) AS session_requester_id,
        ROUND((UNIX_TIMESTAMP(MAX(opened_at)) - UNIX_TIMESTAMP(MIN(opened_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY agent_id, session_no
),
filtered AS (
    -- Compare against the overall max
    SELECT *
    FROM per_session
    WHERE duration_min < (SELECT MAX(duration_min) FROM per_session)
)
-- Final projection
SELECT
    agent_id,
    session_no,
    session_start,
    session_end,
    event_count,
    session_requester_id,
    duration_min,
    GREATEST(duration_min, session_no) AS greatest_duration_min_session_no
FROM filtered
ORDER BY agent_id DESC, session_no
LIMIT 10
