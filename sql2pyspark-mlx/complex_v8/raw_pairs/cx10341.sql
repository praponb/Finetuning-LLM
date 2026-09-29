WITH scoped AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        priority,
        csat,
        tags,
        UPPER(TRIM(priority)) AS priority_clean
    FROM tickets
    WHERE NOT ARRAY_CONTAINS(tags, 'outage')
),
combined_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.agent_id,
        s.priority,
        s.tags,
        su.agent_name
    FROM scoped s
    JOIN support_agents su ON su.id = s.agent_id
    WHERE su.agent_name IS NOT NULL
),
qualified AS (
    -- Keep rows whose agent matches
    SELECT s.*
    FROM combined_details s
    WHERE EXISTS (
            SELECT 1
            FROM support_agents su
            WHERE su.id = s.agent_id
              AND su.agent_name IS NOT NULL
              AND su.team NOT IN ('Billing', 'Tech')
        )
)
-- Subtotals with ROLLUP
SELECT
    agent_id,
    priority,
    GROUPING_ID() AS gid,
    COUNT(DISTINCT agent_id) AS distinct_agent_ids,
    COUNT(*) FILTER (WHERE priority = 'P1') AS n_priority_p1
FROM qualified
GROUP BY ROLLUP(agent_id, priority)
