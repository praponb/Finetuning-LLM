SELECT 'agent' AS src, id, tier AS name
FROM support_agents
WHERE YEAR(hired_on) = 2024
  AND team = 'VIP'
UNION ALL
SELECT 'agent_2' AS src, id, agent_name
FROM support_agents
ORDER BY id
