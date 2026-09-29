WITH base_tickets AS (
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
        tags,
        ABS(requester_id - 13) AS requester_id_distance
    FROM tickets
),
with_prev AS (
    -- Previous opened_at per priority
    SELECT *, LAG(opened_at) OVER (PARTITION BY priority ORDER BY opened_at, id) AS prev_opened_at
    FROM base_tickets
),
flagged AS (
    -- Flag rows that start a new session (gap > 90 minutes)
    SELECT *, CASE WHEN prev_opened_at IS NULL OR (UNIX_TIMESTAMP(opened_at) - UNIX_TIMESTAMP(prev_opened_at)) / 60 > 90 THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY priority ORDER BY opened_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
)
-- Final projection
SELECT
    id,
    requester_id,
    opened_at,
    closed_at,
    csat,
    prev_opened_at,
    is_new_session,
    session_id,
    FLOOR(requester_id / 5) * 5 AS requester_id_bucket
FROM sessionized
WHERE requester_id <= 19
ORDER BY session_id DESC NULLS FIRST
