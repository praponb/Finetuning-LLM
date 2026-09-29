WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        csat,
        tags
    FROM tickets
),
with_prev AS (
    -- Previous opened_at per agent_id
    SELECT *, LAG(opened_at) OVER (PARTITION BY agent_id ORDER BY opened_at, id) AS prev_opened_at
    FROM valid
),
flagged AS (
    -- Flag rows that start a new session (gap > 30 minutes)
    SELECT *, CASE WHEN prev_opened_at IS NULL OR opened_at > prev_opened_at + INTERVAL 30 MINUTES THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY agent_id ORDER BY opened_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
)
-- Final projection
SELECT
    id,
    requester_id,
    opened_at,
    closed_at,
    csat,
    tags,
    prev_opened_at,
    is_new_session,
    session_id,
    TO_DATE(prev_opened_at) AS prev_opened_at_date
FROM sessionized
ORDER BY csat
