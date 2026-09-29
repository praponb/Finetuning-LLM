WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        priority,
        tags,
        closed_at + INTERVAL 30 MINUTES AS closed_at_plus_30m
    FROM tickets
    WHERE channel IN ('phone', 'web')
),
with_parent AS (
    -- Enrich with dimension data
    SELECT s.*, su.tier AS agent_tier
    FROM prepared s
    JOIN support_agents su ON su.id = s.agent_id
)
SELECT
    agent_tier,
    agent_id,
    GROUPING(agent_tier) AS is_total_agent_tier,
    MIN(requester_id) AS min_requester_id
FROM with_parent
GROUP BY CUBE(agent_tier, agent_id)
