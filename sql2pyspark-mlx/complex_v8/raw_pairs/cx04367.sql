SELECT id
FROM tickets
INTERSECT
SELECT id
FROM support_agents
WHERE YEAR(hired_on) = 2024
  AND tier = 'T1'
ORDER BY id DESC
