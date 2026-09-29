SELECT channel, id AS entity_id, 'ticket' AS origin
FROM tickets
UNION ALL
SELECT tier AS channel, id AS entity_id, 'agent' AS origin
FROM support_agents
WHERE tier = 'T1'
