SELECT id AS key_id, hired_on AS day
FROM support_agents
UNION ALL
SELECT id AS key_id, hired_on AS day
FROM support_agents
ORDER BY key_id DESC, day
