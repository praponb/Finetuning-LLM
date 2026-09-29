WITH scoped_tickets AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        channel,
        tags
    FROM tickets
),
with_prev AS (
    -- Previous closed_at per channel
    SELECT *, LAG(closed_at) OVER (PARTITION BY channel ORDER BY closed_at, id) AS prev_closed_at
    FROM scoped_tickets
),
flagged AS (
    -- Flag rows that start a new session (gap > 60 minutes)
    SELECT *, CASE WHEN prev_closed_at IS NULL OR closed_at > prev_closed_at + INTERVAL 60 MINUTES THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY channel ORDER BY closed_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
)
-- Subtotals with CUBE
SELECT
    COALESCE(channel, 'ALL') AS channel,
    burst_id,
    MIN(prev_closed_at) AS first_prev_closed_at
FROM sessionized
GROUP BY CUBE(channel, burst_id)
ORDER BY channel NULLS LAST, burst_id NULLS LAST
