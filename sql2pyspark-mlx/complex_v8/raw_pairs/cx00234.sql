WITH valid_tickets AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        tags,
        LOWER(TRIM(channel)) AS channel_clean
    FROM tickets
),
with_prev AS (
    -- Previous opened_at per channel
    SELECT *, LAG(opened_at) OVER (PARTITION BY channel ORDER BY opened_at, id) AS prev_opened_at
    FROM valid_tickets
),
flagged AS (
    -- Flag rows that start a new session (gap > 30 minutes)
    SELECT *, CASE WHEN prev_opened_at IS NULL OR (UNIX_TIMESTAMP(opened_at) - UNIX_TIMESTAMP(prev_opened_at)) / 60 > 30 THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY channel ORDER BY opened_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT burst_id, agent_id, channel, requester_id
    FROM sessionized
) src
PIVOT (
    MIN(requester_id)
    FOR channel IN ('phone', 'web', 'email', 'chat')
)
ORDER BY burst_id, agent_id
