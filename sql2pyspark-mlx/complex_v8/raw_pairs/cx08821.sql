SELECT id
FROM support_agents
WHERE LOWER(agent_name) LIKE '%ng%'
  AND tier IN ('T2', 'T1')
MINUS
SELECT id
FROM support_agents
ORDER BY id DESC
