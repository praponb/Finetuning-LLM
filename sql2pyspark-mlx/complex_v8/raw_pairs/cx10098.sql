WITH filtered AS (
    -- Scope the support_agents rows
    SELECT
        id,
        agent_name,
        team,
        tier,
        hired_on,
        CASE WHEN team IN ('Billing', 'Tech') THEN 'group_a' WHEN team = 'Onboarding' THEN 'group_b' ELSE 'other' END AS team_group,
        MONTH(hired_on) AS hired_on_month
    FROM support_agents
    WHERE hired_on < DATE '2025-02-27'
      AND agent_name IS NOT NULL
),
derived AS (
    -- Derive helper columns
    SELECT
        *,
        SUBSTRING(TRIM(agent_name), 1, 4) AS agent_name_prefix,
        YEAR(hired_on) AS hired_on_year,
        ABS(hired_on_month - 9) AS hired_on_month_distance
    FROM filtered
)
-- Subtotals with ROLLUP
SELECT
    tier,
    team_group,
    hired_on_month,
    MAX(hired_on) AS last_hired_on,
    ROUND(STDDEV(hired_on_year), 2) AS stddev_hired_on_year,
    ROUND(AVG(hired_on_month_distance), 2) AS avg_hired_on_month_distance
FROM derived
GROUP BY ROLLUP(tier, team_group, hired_on_month)
ORDER BY tier NULLS LAST, team_group NULLS LAST, hired_on_month NULLS LAST
