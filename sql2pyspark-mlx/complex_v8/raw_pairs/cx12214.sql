WITH scoped AS (
    -- Scope the support_agents rows
    SELECT
        id,
        agent_name,
        team,
        tier,
        DATE_FORMAT(hired_on, 'yyyy-MM') AS hired_on_label,
        SUBSTRING(TRIM(tier), 1, 4) AS tier_prefix,
        CASE WHEN tier IN ('T2', 'T3') THEN 'group_a' WHEN tier = 'T1' THEN 'group_b' ELSE 'other' END AS tier_group
    FROM support_agents
    WHERE tier = 'T1'
),
scored AS (
    -- Label and score rows
    SELECT
        *,
        LOWER(REGEXP_REPLACE(TRIM(tier_group), ' ', '_')) AS tier_group_slug,
        LOWER(REGEXP_REPLACE(TRIM(tier_group), ' ', '_')) AS tier_group_slug_2,
        CONCAT_WS('|', agent_name, team) AS agent_name_team_label
    FROM scoped
),
labeled AS (
    -- Derive helper columns
    SELECT
        *,
        CONCAT_WS('|', tier_group, tier) AS tier_group_tier_label,
        LOWER(REGEXP_REPLACE(TRIM(agent_name), ' ', '_')) AS agent_name_slug
    FROM scored
)
-- Subtotals with ROLLUP
SELECT
    tier,
    team,
    GROUPING(tier) AS is_total_tier,
    ARRAY_SORT(COLLECT_SET(agent_name)) AS agent_name_list,
    SUM(CASE WHEN team IS NOT NULL THEN 1 ELSE 0 END) AS n_team_is_not_null,
    COUNT(*) FILTER (WHERE tier <> 'T1') AS n_tier_t1
FROM labeled
GROUP BY ROLLUP(tier, team)
