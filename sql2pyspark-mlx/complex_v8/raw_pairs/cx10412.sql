WITH scoped AS (
    -- Scope the tickets rows
    SELECT
        id,
        agent_id,
        closed_at,
        priority,
        tags,
        DATEDIFF(DATE '2025-12-31', closed_at) AS days_since_closed_at
    FROM tickets
    WHERE agent_id IS NOT NULL
      AND csat < 5
),
joined AS (
    -- Join support_agents
    SELECT
        s.id,
        s.agent_id,
        s.closed_at,
        s.priority,
        s.tags,
        s.days_since_closed_at,
        su.tier AS agent_tier
    FROM scoped s
    LEFT JOIN support_agents su ON su.id = s.agent_id
),
with_details AS (
    -- Attach agent attributes
    SELECT
        s.id,
        s.agent_id,
        s.closed_at,
        s.priority,
        s.days_since_closed_at,
        s.agent_tier,
        su.hired_on AS agent_hired_on,
        su.agent_name
    FROM joined s
    JOIN support_agents su ON su.id = s.agent_id
)
SELECT
    id,
    agent_id,
    closed_at,
    priority,
    agent_tier,
    agent_hired_on,
    agent_name
FROM with_details
ORDER BY agent_hired_on
