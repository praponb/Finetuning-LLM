WITH prepared_tickets AS (
    -- Filter tickets
    SELECT
        id,
        agent_id,
        closed_at,
        priority,
        channel
    FROM tickets
    WHERE ARRAY_CONTAINS(tags, 'upgrade')
),
joined_agent AS (
    -- Attach agent attributes
    SELECT
        s.id,
        s.agent_id,
        s.priority,
        su.tier AS agent_tier
    FROM prepared_tickets s
    JOIN support_agents su ON su.id = s.agent_id
    WHERE su.tier IS NOT NULL
),
with_details_agent AS (
    -- Attach agent attributes
    SELECT s.*, su.hired_on AS agent_hired_on, su.agent_name
    FROM joined_agent s
    JOIN support_agents su ON su.id = s.agent_id
)
SELECT
    id,
    agent_id,
    agent_tier,
    agent_hired_on,
    agent_name,
    DATEDIFF(DATE '2025-12-31', agent_hired_on) AS days_since_agent_hired_on,
    COALESCE(priority, 'UNKNOWN') AS priority_or_unknown
FROM with_details_agent
ORDER BY agent_hired_on, agent_name DESC NULLS FIRST
