SELECT 'agent' AS record_type, id AS qty, hired_on AS day
FROM support_agents
WHERE tier = 'T3'
UNION ALL
SELECT 'agent_2' AS record_type, id, hired_on
FROM support_agents
