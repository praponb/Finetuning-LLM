SELECT id AS entity_id, tier
FROM support_agents
WHERE team NOT IN ('Billing', 'Tech')
  AND UPPER(TRIM(agent_name)) LIKE 'H%'
INTERSECT
SELECT id AS entity_id, tier
FROM support_agents
ORDER BY entity_id DESC
