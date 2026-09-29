SELECT priority
FROM tickets
INTERSECT
SELECT tier
FROM support_agents
ORDER BY priority
