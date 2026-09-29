SELECT tier AS label
FROM support_agents
EXCEPT
SELECT channel AS label
FROM tickets
WHERE NOT ARRAY_CONTAINS(tags, 'outage')
