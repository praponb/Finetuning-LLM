WITH prepared AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        channel,
        tags,
        COALESCE(requester_id, 0) AS requester_id_filled
    FROM tickets
    WHERE channel IN ('phone', 'chat')
),
with_prev AS (
    -- Previous closed_at per agent_id
    SELECT *, LAG(closed_at) OVER (PARTITION BY agent_id ORDER BY closed_at, id) AS prev_closed_at
    FROM prepared
),
flagged AS (
    -- Flag rows that start a new session (gap > 90 minutes)
    SELECT *, CASE WHEN prev_closed_at IS NULL OR (UNIX_TIMESTAMP(closed_at) - UNIX_TIMESTAMP(prev_closed_at)) / 60 > 90 THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY agent_id ORDER BY closed_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
)
-- Final projection
SELECT
    id,
    requester_id,
    agent_id,
    closed_at,
    channel,
    requester_id_filled,
    prev_closed_at,
    burst_id,
    SUBSTRING(TRIM(channel), 1, 3) AS channel_prefix
FROM sessionized
ORDER BY burst_id DESC
