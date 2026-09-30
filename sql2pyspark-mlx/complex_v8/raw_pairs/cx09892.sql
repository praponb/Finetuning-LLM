WITH prepared AS (
    -- Scope the tickets rows
    SELECT
        id,
        agent_id,
        opened_at,
        priority,
        tags
    FROM tickets
    WHERE NOT ARRAY_CONTAINS(tags, 'upgrade')
),
with_parent_agent AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.agent_id,
        su.agent_name,
        su.team AS agent_team
    FROM prepared s
    JOIN support_agents su ON su.id = s.agent_id
),
enriched_agent AS (
    -- Join support_agents
    SELECT
        s.id,
        s.agent_id,
        s.agent_name,
        s.agent_team,
        su.tier AS agent_tier,
        su.agent_name AS agent_agent_name
    FROM with_parent_agent s
    JOIN support_agents su ON su.id = s.agent_id
)
SELECT
    agent_id,
    agent_tier,
    COUNT(DISTINCT id) AS distinct_ids
FROM enriched_agent
GROUP BY GROUPING SETS ((agent_id, agent_tier), (agent_id), (agent_tier), ())
ORDER BY agent_id NULLS LAST, agent_tier NULLS LAST
