SELECT id AS key_id, agent_name AS label, hired_on
FROM support_agents
UNION ALL
SELECT id AS key_id, team AS label, hired_on
FROM support_agents
ORDER BY hired_on
