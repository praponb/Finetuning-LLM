SELECT id AS entity_id, agent_name AS name
FROM support_agents
UNION ALL
SELECT id AS entity_id, team AS name
FROM support_agents
WHERE LOWER(agent_name) LIKE '%en%'
