SELECT id, team, hired_on
FROM support_agents
UNION
SELECT id, agent_name AS team, hired_on
FROM support_agents
