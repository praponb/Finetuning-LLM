SELECT id
FROM support_agents
WHERE team IS NOT NULL
INTERSECT
SELECT id
FROM support_agents
