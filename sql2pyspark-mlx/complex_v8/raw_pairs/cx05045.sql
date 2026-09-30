SELECT id, agent_name AS category
FROM support_agents
EXCEPT
SELECT id, tier
FROM support_agents
WHERE hired_on >= DATE '2024-07-28'
  AND LOWER(agent_name) LIKE '%is%'
ORDER BY category DESC
