WITH valid AS (
    -- Filter tickets
    SELECT
        id,
        agent_id,
        closed_at,
        priority,
        tags
    FROM tickets
    WHERE SIZE(tags) >= 1
      AND priority IS NOT NULL
),
rollup_base AS (
    -- Group-level totals
    SELECT
        agent_id,
        YEAR(closed_at) AS closed_at_year,
        priority,
        COUNT(*) FILTER (WHERE priority = 'P4') AS n_priority_p4,
        ARRAY_SORT(COLLECT_SET(priority)) AS priority_list,
        COUNT(DISTINCT id) AS distinct_ids,
        ROUND(100 * COUNT(CASE WHEN priority = 'P2' THEN 1 END) / COUNT(*), 1) AS pct_priority_p2,
        COUNT(*) AS n_rows
    FROM valid
    WHERE priority = 'P2'
    GROUP BY agent_id, YEAR(closed_at), priority
),
combined_details AS (
    -- Join support_agents
    SELECT
        s.agent_id,
        s.closed_at_year,
        s.priority,
        s.pct_priority_p2,
        s.n_rows,
        su.hired_on AS agent_hired_on,
        su.tier AS agent_tier
    FROM rollup_base s
    JOIN support_agents su ON su.id = s.agent_id
)
-- Final projection
SELECT
    agent_id,
    closed_at_year,
    priority,
    pct_priority_p2,
    n_rows,
    agent_hired_on,
    agent_tier
FROM combined_details
ORDER BY n_rows DESC, pct_priority_p2 DESC
