WITH base_tickets AS (
    -- Keep valid rows and derive helper columns
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
        TO_DATE(opened_at) AS opened_at_date
    FROM tickets
    WHERE agent_id >= 8
),
with_prev AS (
    -- Previous opened_at per channel
    SELECT *, LAG(opened_at) OVER (PARTITION BY channel ORDER BY opened_at, id) AS prev_opened_at
    FROM base_tickets
),
flagged AS (
    -- Flag rows that start a new session (gap > 30 minutes)
    SELECT *, CASE WHEN prev_opened_at IS NULL OR opened_at > prev_opened_at + INTERVAL 30 MINUTES THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY channel ORDER BY opened_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
screened AS (
    -- Compare against the overall avg
    SELECT *
    FROM sessionized
    WHERE burst_id > (SELECT AVG(burst_id) FROM sessionized)
)
-- Final projection
SELECT
    id,
    requester_id,
    agent_id,
    opened_at,
    channel,
    csat,
    opened_at_date,
    prev_opened_at,
    starts_session,
    burst_id,
    LEAST(starts_session, csat) AS least_starts_session_csat
FROM screened
WHERE opened_at < TIMESTAMP '2025-02-12 14:00:00'
ORDER BY opened_at NULLS LAST
