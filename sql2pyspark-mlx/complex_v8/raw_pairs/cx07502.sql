SELECT id, hired_on, tier AS name
FROM support_agents
UNION ALL
SELECT id, hired_on, agent_name
FROM support_agents
WHERE tier = 'T1'
ORDER BY hired_on DESC
