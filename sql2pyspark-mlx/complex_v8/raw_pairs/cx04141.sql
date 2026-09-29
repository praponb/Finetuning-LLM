SELECT
    id AS entity_id,
    tier AS category,
    hired_on,
    'agent' AS src
FROM support_agents
WHERE hired_on < DATE '2024-12-31'
  AND UPPER(TRIM(agent_name)) LIKE 'G%'
UNION
SELECT
    id AS entity_id,
    agent_name AS category,
    hired_on,
    'agent_2' AS src
FROM support_agents
WHERE LOWER(agent_name) LIKE '%jo%'
  AND hired_on < DATE '2025-03-30'
ORDER BY src, hired_on
