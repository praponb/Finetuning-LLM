WITH filtered_tickets AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        csat
    FROM tickets
),
with_activity AS (
    -- Rows with no matching tickets
    SELECT s.*
    FROM filtered_tickets s
    WHERE NOT EXISTS (
            SELECT 1
            FROM tickets t
            WHERE t.agent_id = s.agent_id
              AND HOUR(t.closed_at) BETWEEN 9 AND 13
              AND t.opened_at >= TIMESTAMP '2025-01-25 11:00:00'
        )
),
with_parent_agent AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.agent_id,
        s.opened_at,
        s.closed_at,
        s.priority,
        s.csat,
        su.tier AS agent_tier,
        su.agent_name,
        su.team AS agent_team
    FROM with_activity s
    LEFT JOIN support_agents su ON su.id = s.agent_id
),
with_parent_agent_2 AS (
    -- Attach agent attributes
    SELECT
        s.id,
        s.agent_id,
        s.closed_at,
        s.priority,
        s.csat,
        s.agent_name,
        s.agent_team,
        su.hired_on AS agent_hired_on
    FROM with_parent_agent s
    JOIN support_agents su ON su.id = s.agent_id
),
with_parent_agent_3 AS (
    -- Join support_agents
    SELECT
        s.id,
        s.agent_id,
        s.closed_at,
        s.csat,
        s.agent_name,
        s.agent_hired_on,
        su.hired_on AS agent_hired_on_2,
        su.agent_name AS agent_agent_name
    FROM with_parent_agent_2 s
    LEFT JOIN support_agents su ON su.id = s.agent_id AND YEAR(su.hired_on) = 2024
)
-- Final projection
SELECT
    id,
    closed_at,
    csat,
    agent_name,
    agent_hired_on,
    agent_hired_on_2,
    agent_agent_name,
    CONCAT(agent_name, '#', CAST(csat AS STRING)) AS agent_name_ref
FROM with_parent_agent_3
ORDER BY agent_hired_on_2, id
LIMIT 20
