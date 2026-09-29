WITH base_tickets AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        priority,
        channel,
        csat,
        tags,
        COALESCE(priority, 'UNKNOWN') AS priority_or_unknown
    FROM tickets
),
with_prev AS (
    -- Previous closed_at per priority_or_unknown
    SELECT *, LAG(closed_at) OVER (PARTITION BY priority_or_unknown ORDER BY closed_at, id) AS prev_closed_at
    FROM base_tickets
),
flagged AS (
    -- Flag rows that start a new session (gap > 45 minutes)
    SELECT *, CASE WHEN prev_closed_at IS NULL OR closed_at > prev_closed_at + INTERVAL 45 MINUTES THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY priority_or_unknown ORDER BY closed_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
)
-- Final projection
SELECT
    id,
    requester_id,
    closed_at,
    priority,
    csat,
    tags,
    priority_or_unknown,
    prev_closed_at,
    session_id
FROM sessionized
