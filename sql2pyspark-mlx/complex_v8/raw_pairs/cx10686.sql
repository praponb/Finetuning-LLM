SELECT
    'agent' AS src,
    agent_name,
    tier,
    hired_on
FROM support_agents
UNION ALL
SELECT
    'agent_2' AS src,
    team,
    tier,
    hired_on
FROM support_agents
WHERE YEAR(hired_on) = 2024
  AND team <> 'VIP'
ORDER BY hired_on, src
