WITH filtered_tickets AS (
    -- Scope the tickets rows
    SELECT
        id,
        agent_id,
        opened_at,
        closed_at,
        csat,
        tags
    FROM tickets
    WHERE closed_at < TIMESTAMP '2025-02-19 21:00:00'
      AND ARRAY_CONTAINS(tags, 'bug')
),
joined AS (
    -- Join support_agents
    SELECT
        s.id,
        s.agent_id,
        s.opened_at,
        s.closed_at,
        s.tags,
        su.hired_on AS agent_hired_on,
        su.tier AS agent_tier
    FROM filtered_tickets s
    JOIN support_agents su ON su.id = s.agent_id
    WHERE su.tier IN ('T3', 'T2')
)
-- Subtotals with GROUPING SETS
SELECT
    agent_tier,
    agent_id,
    MAX(closed_at) AS last_closed_at,
    COUNT(*) FILTER (WHERE agent_tier NOT IN ('T3', 'T2')) AS n_agent_tier_not_in_t3_t2
FROM joined
GROUP BY GROUPING SETS ((agent_tier, agent_id), (agent_tier), (agent_id), ())
ORDER BY agent_tier NULLS LAST, agent_id NULLS LAST
