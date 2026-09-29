SELECT agent_name AS category
FROM support_agents
WHERE tier <> 'T2'
  AND hired_on < DATE '2025-02-26'
EXCEPT
SELECT team AS category
FROM support_agents
ORDER BY category
