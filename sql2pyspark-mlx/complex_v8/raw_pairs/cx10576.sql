SELECT 'agent' AS src, id, hired_on
FROM support_agents
WHERE hired_on < DATE '2025-02-27'
  AND agent_name IS NOT NULL
UNION
SELECT 'agent_2' AS src, id, hired_on
FROM support_agents
ORDER BY id
