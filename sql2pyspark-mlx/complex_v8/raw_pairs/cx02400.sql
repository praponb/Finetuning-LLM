WITH scoped AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        tags
    FROM tickets
    WHERE priority <> 'P4'
),
with_prev AS (
    -- Previous opened_at per priority
    SELECT *, LAG(opened_at) OVER (PARTITION BY priority ORDER BY opened_at, id) AS prev_opened_at
    FROM scoped
),
flagged AS (
    -- Flag rows that start a new session (gap > 45 minutes)
    SELECT *, CASE WHEN prev_opened_at IS NULL OR opened_at > prev_opened_at + INTERVAL 45 MINUTES THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY priority ORDER BY opened_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
)
SELECT
    id,
    agent_id,
    opened_at,
    priority,
    tags,
    prev_opened_at,
    new_session_flag,
    burst_id,
    SUBSTRING(TRIM(priority), 1, 4) AS priority_prefix,
    ABS(burst_id - 4) AS burst_id_distance
FROM sessionized
ORDER BY new_session_flag, burst_id_distance NULLS LAST
