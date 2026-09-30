WITH prepared AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        tags,
        CONCAT(priority, '#', CAST(requester_id AS STRING)) AS priority_ref
    FROM tickets
),
with_prev AS (
    -- Previous opened_at per priority
    SELECT *, LAG(opened_at) OVER (PARTITION BY priority ORDER BY opened_at, id) AS prev_opened_at
    FROM prepared
),
flagged AS (
    -- Flag rows that start a new session (gap > 90 minutes)
    SELECT *, CASE WHEN prev_opened_at IS NULL OR (UNIX_TIMESTAMP(opened_at) - UNIX_TIMESTAMP(prev_opened_at)) / 60 > 90 THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY priority ORDER BY opened_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
enriched_agent AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.opened_at,
        s.closed_at,
        s.priority,
        s.channel,
        s.tags,
        s.new_session_flag,
        su.agent_name,
        su.hired_on AS agent_hired_on
    FROM sessionized s
    JOIN support_agents su ON su.id = s.agent_id
),
labeled AS (
    -- Derive helper columns
    SELECT
        id,
        requester_id,
        closed_at,
        priority,
        tags,
        new_session_flag,
        agent_name,
        DATEDIFF(DATE '2025-12-31', closed_at) AS days_since_closed_at
    FROM enriched_agent
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, measure, value
FROM (
    SELECT id, new_session_flag, requester_id
    FROM labeled
) src
UNPIVOT (
    value FOR measure IN (new_session_flag, requester_id)
) unp
