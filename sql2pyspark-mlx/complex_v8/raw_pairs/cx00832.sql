SELECT id AS entity_id, hired_on AS day, 'agent' AS record_type
FROM support_agents
WHERE team <> 'Billing'
  AND UPPER(TRIM(agent_name)) LIKE 'H%'
UNION ALL
SELECT id AS entity_id, hired_on AS day, 'agent_2' AS record_type
FROM support_agents
WHERE team IS NOT NULL
ORDER BY entity_id
