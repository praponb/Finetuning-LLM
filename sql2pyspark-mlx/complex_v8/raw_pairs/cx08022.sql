WITH cleaned_agents AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        agent_name,
        team,
        tier,
        SUBSTRING(TRIM(agent_name), 1, 3) AS agent_name_prefix
    FROM support_agents
    WHERE (hired_on BETWEEN DATE '2024-01-30' AND DATE '2025-03-30' OR tier IN ('T3', 'T1'))
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        tier,
        team,
        COUNT(*) FILTER (WHERE team IS NOT NULL) AS n_team_is_not_null,
        SUM(CASE WHEN LOWER(agent_name) LIKE '%s %' THEN 1 ELSE 0 END) AS n_lower_agent_name_like_s,
        COUNT(*) AS row_count,
        COUNT(DISTINCT id) AS distinct_ids,
        COUNT(*) FILTER (WHERE tier IS NOT NULL) AS n_tier_is_not_null
    FROM cleaned_agents
    GROUP BY tier, team
)
SELECT
    tier,
    team,
    n_team_is_not_null,
    n_lower_agent_name_like_s,
    distinct_ids
FROM grouped
WHERE n_team_is_not_null < 9
ORDER BY team DESC
