SELECT tier
FROM support_agents
WHERE YEAR(hired_on) = 2024
INTERSECT
SELECT tier
FROM support_agents
ORDER BY tier
