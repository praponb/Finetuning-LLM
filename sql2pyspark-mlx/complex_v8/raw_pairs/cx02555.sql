WITH filtered AS (
    -- Scope the support_agents rows
    SELECT
        id,
        agent_name,
        team,
        tier,
        hired_on,
        LENGTH(TRIM(tier)) AS tier_len,
        LOWER(TRIM(agent_name)) AS agent_name_clean
    FROM support_agents
    WHERE hired_on < DATE '2025-03-30'
      AND LENGTH(TRIM(agent_name)) >= 6
),
tagged AS (
    -- Derive helper columns
    SELECT
        id,
        agent_name,
        team,
        hired_on,
        agent_name_clean,
        CASE WHEN tier IN ('T3', 'T1') THEN 'group_a' WHEN tier = 'T2' THEN 'group_b' ELSE 'other' END AS tier_group,
        LENGTH(TRIM(agent_name)) AS agent_name_len,
        ABS(tier_len - 6) AS tier_len_distance
    FROM filtered
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(tier_len_distance - AVG(tier_len_distance) OVER w_agent_name_len_tier_group, 2) AS tier_len_distance_vs_agent_name_len_tier_group_a,
        ROUND(agent_name_len / NULLIF(SUM(agent_name_len) OVER (PARTITION BY agent_name_len, tier_group), 0), 4) AS agent_name_len_share_of_agent_name_len_tier_grou,
        agent_name_len - LAG(agent_name_len) OVER (PARTITION BY tier_group ORDER BY agent_name_len NULLS LAST, id) AS agent_name_len_change
    FROM tagged
    WINDOW w_agent_name_len_tier_group AS (PARTITION BY agent_name_len, tier_group)
)
-- Subtotals with GROUPING SETS
SELECT
    COALESCE(team, 'ALL') AS team,
    agent_name_len,
    COALESCE(tier_group, 'ALL') AS tier_group,
    COUNT(*) AS row_count,
    SUM(CASE WHEN team = 'Billing' THEN agent_name_len ELSE 0 END) AS agent_name_len_team_billing
FROM with_trends
GROUP BY GROUPING SETS ((team, agent_name_len, tier_group), (team), (agent_name_len))
