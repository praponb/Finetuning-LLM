WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        agent_id,
        opened_at,
        closed_at,
        tags
    FROM tickets
),
with_parent_agent AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.agent_id,
        s.opened_at,
        s.closed_at,
        su.team AS agent_team
    FROM base s
    LEFT JOIN support_agents su ON su.id = s.agent_id
),
per_group AS (
    -- Group-level totals
    SELECT
        DATE_FORMAT(opened_at, 'yyyy-MM') AS opened_at_ym,
        SUM(CASE WHEN agent_team = 'VIP' THEN 1 ELSE 0 END) AS n_agent_team_vip,
        ARRAY_SORT(COLLECT_SET(agent_team)) AS agent_team_list,
        COUNT(DISTINCT agent_id) AS distinct_agent_ids,
        COUNT(*) AS row_count,
        MIN(opened_at) AS first_opened_at
    FROM with_parent_agent
    WHERE agent_id IS NOT NULL
    GROUP BY DATE_FORMAT(opened_at, 'yyyy-MM')
),
without_matches AS (
    -- Compare against the overall min
    SELECT *
    FROM per_group
    WHERE distinct_agent_ids > (SELECT MIN(distinct_agent_ids) FROM per_group)
),
without_matches_2 AS (
    -- Compare against the overall avg
    SELECT *
    FROM without_matches
    WHERE row_count > (SELECT AVG(row_count) FROM without_matches)
)
-- Final projection
SELECT
    opened_at_ym,
    agent_team_list,
    distinct_agent_ids,
    row_count
FROM without_matches_2
ORDER BY opened_at_ym DESC, row_count DESC NULLS FIRST
