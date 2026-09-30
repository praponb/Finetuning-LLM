SELECT id AS ref_id, hired_on AS day
FROM support_agents
WHERE UPPER(TRIM(agent_name)) LIKE 'H%'
  AND hired_on BETWEEN DATE '2024-07-09' AND DATE '2025-02-26'
UNION ALL
SELECT id, hired_on
FROM support_agents
ORDER BY day
