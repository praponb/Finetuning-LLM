SELECT tier AS name
FROM support_agents
WHERE LENGTH(TRIM(agent_name)) >= 8
  AND hired_on BETWEEN DATE '2024-05-02' AND DATE '2025-02-26'
INTERSECT
SELECT agent_name AS name
FROM support_agents
