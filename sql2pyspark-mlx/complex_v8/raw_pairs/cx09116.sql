SELECT channel AS name
FROM tickets
EXCEPT
SELECT team AS name
FROM support_agents
ORDER BY name DESC
