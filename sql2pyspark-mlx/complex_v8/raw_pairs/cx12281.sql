WITH recent AS (
    -- Scope the support_agents rows
    SELECT
        id,
        agent_name,
        tier,
        hired_on,
        DATEDIFF(DATE '2025-06-30', hired_on) AS days_since_hired_on,
        CASE WHEN team IN ('Billing', 'Tech') THEN 'group_a' WHEN team = 'VIP' THEN 'group_b' ELSE 'other' END AS team_group
    FROM support_agents
    WHERE (hired_on BETWEEN DATE '2024-04-14' AND DATE '2025-02-27' OR UPPER(TRIM(agent_name)) LIKE 'I%')
),
qualified AS (
    -- Compare against the overall avg
    SELECT *
    FROM recent
    WHERE days_since_hired_on > (SELECT AVG(days_since_hired_on) FROM recent)
),
stats AS (
    -- Summarise per group
    SELECT team_group, COUNT(*) AS row_count, COUNT(DISTINCT tier) AS distinct_tiers
    FROM qualified
    GROUP BY team_group
)
SELECT
    team_group,
    row_count,
    GROUPING_ID() AS gid,
    ROUND(STDDEV(distinct_tiers), 2) AS stddev_distinct_tiers,
    COUNT(*) AS n_rows
FROM stats
GROUP BY CUBE(team_group, row_count)
ORDER BY team_group NULLS LAST, row_count NULLS LAST
