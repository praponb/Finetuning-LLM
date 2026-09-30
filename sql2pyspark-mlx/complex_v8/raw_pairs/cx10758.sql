SELECT id
FROM support_agents
INTERSECT ALL
SELECT ticket_id
FROM ticket_events
WHERE actor NOT IN ('agent', 'customer')
ORDER BY id
