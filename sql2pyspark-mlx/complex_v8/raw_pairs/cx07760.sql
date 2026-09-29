SELECT id AS qty, team
FROM support_agents
EXCEPT
SELECT id, agent_name
FROM support_agents
