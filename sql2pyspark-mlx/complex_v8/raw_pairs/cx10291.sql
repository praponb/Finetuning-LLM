WITH scoped_tickets AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        channel,
        tags,
        ABS(csat - 2) AS csat_distance
    FROM tickets
    WHERE SIZE(tags) >= 1
),
with_prev AS (
    -- Previous opened_at per channel
    SELECT *, LAG(opened_at) OVER (PARTITION BY channel ORDER BY opened_at, id) AS prev_opened_at
    FROM scoped_tickets
),
flagged AS (
    -- Flag rows that start a new session (gap > 30 minutes)
    SELECT *, CASE WHEN prev_opened_at IS NULL OR opened_at > prev_opened_at + INTERVAL 30 MINUTES THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY channel ORDER BY opened_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
)
-- Final projection
SELECT
    id,
    requester_id,
    opened_at,
    channel,
    csat_distance,
    prev_opened_at,
    new_session_flag
FROM sessionized
ORDER BY id DESC, prev_opened_at
