WITH filtered AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        priority,
        channel,
        csat,
        tags,
        DAYOFWEEK(opened_at) AS opened_at_dayofweek
    FROM tickets
),
with_prev AS (
    -- Previous closed_at per priority
    SELECT *, LAG(closed_at) OVER (PARTITION BY priority ORDER BY closed_at, id) AS prev_closed_at
    FROM filtered
),
flagged AS (
    -- Flag rows that start a new session (gap > 45 minutes)
    SELECT *, CASE WHEN prev_closed_at IS NULL OR closed_at > prev_closed_at + INTERVAL 45 MINUTES THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY priority ORDER BY closed_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
session_summary AS (
    -- One row per session
    SELECT
        priority,
        session_id,
        MIN(closed_at) AS session_start,
        MAX(closed_at) AS session_end,
        COUNT(*) AS event_count,
        SUM(requester_id) AS session_requester_id,
        ROUND((UNIX_TIMESTAMP(MAX(closed_at)) - UNIX_TIMESTAMP(MIN(closed_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY priority, session_id
),
qualified AS (
    -- Compare against the overall avg
    SELECT *
    FROM session_summary
    WHERE session_requester_id > (SELECT AVG(session_requester_id) FROM session_summary)
)
SELECT
    priority,
    session_id,
    session_start,
    event_count,
    session_requester_id
FROM qualified
ORDER BY session_id DESC NULLS FIRST
