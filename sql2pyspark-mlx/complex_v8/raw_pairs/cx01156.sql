SELECT id
FROM support_agents
WHERE team IS NOT NULL
  AND tier = 'T2'
EXCEPT ALL
SELECT id
FROM tickets
WHERE channel NOT IN ('web', 'email')
  AND priority = 'P2'
ORDER BY id
