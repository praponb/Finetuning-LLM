SELECT id AS entity_id, hired_on AS event_date
FROM support_agents
UNION ALL
SELECT id, hired_on
FROM support_agents
WHERE agent_name IS NOT NULL
