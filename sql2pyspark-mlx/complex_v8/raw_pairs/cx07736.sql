WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        agent_name,
        team,
        tier,
        hired_on,
        CASE WHEN tier IN ('T1', 'T2') THEN 'group_a' WHEN tier = 'T3' THEN 'group_b' ELSE 'other' END AS tier_group
    FROM support_agents
)
SELECT agent_name AS name, hired_on
FROM recent
WHERE YEAR(hired_on) = 2024
UNION ALL
SELECT team, hired_on
FROM support_agents
WHERE tier NOT IN ('T1', 'T3')
  AND hired_on >= DATE '2024-07-28'
ORDER BY name, hired_on
