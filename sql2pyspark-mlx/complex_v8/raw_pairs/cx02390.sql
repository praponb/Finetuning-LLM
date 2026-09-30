WITH valid_tickets AS (
    -- Filter tickets
    SELECT *
    FROM tickets
    WHERE agent_id <= 2
),
with_prev AS (
    -- Previous opened_at per channel
    SELECT *, LAG(opened_at) OVER (PARTITION BY channel ORDER BY opened_at, id) AS prev_opened_at
    FROM valid_tickets
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
    agent_id,
    opened_at,
    closed_at,
    priority,
    channel,
    tags,
    prev_opened_at,
    new_session_flag,
    SUBSTRING(TRIM(priority), 1, 3) AS priority_prefix,
    CONCAT(priority, '#', CAST(csat AS STRING)) AS priority_ref
FROM sessionized
ORDER BY closed_at, agent_id DESC NULLS FIRST
