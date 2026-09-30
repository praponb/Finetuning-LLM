SELECT id
FROM ticket_events
INTERSECT
SELECT id
FROM support_agents
WHERE hired_on < DATE '2025-02-26'
