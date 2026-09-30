WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        priority,
        tags
    FROM tickets
),
with_prev AS (
    -- Previous opened_at per agent_id
    SELECT *, LAG(opened_at) OVER (PARTITION BY agent_id ORDER BY opened_at, id) AS prev_opened_at
    FROM scoped
),
flagged AS (
    -- Flag rows that start a new session (gap > 2 hours)
    SELECT *, CASE WHEN prev_opened_at IS NULL OR opened_at > prev_opened_at + INTERVAL 2 HOURS THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY agent_id ORDER BY opened_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        *,
        FIRST_VALUE(starts_session) IGNORE NULLS OVER (PARTITION BY starts_session, agent_id ORDER BY starts_session, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_starts_session,
        MAX(burst_id) OVER (PARTITION BY burst_id, priority) AS burst_id_priority_max_burst_id
    FROM sessionized
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, field, reading
FROM (
    SELECT id, starts_session, burst_id_priority_max_burst_id, requester_id
    FROM sequenced
) src
UNPIVOT (
    reading FOR field IN (starts_session, burst_id_priority_max_burst_id, requester_id)
) unp
ORDER BY id, field
