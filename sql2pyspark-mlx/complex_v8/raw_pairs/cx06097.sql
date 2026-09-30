SELECT agent_name, hired_on
FROM support_agents
UNION ALL
SELECT agent_name, hired_on
FROM support_agents
WHERE team = 'Tech'
