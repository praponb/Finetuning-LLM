SELECT actor
FROM ticket_events
WHERE MONTH(event_at) IN (1, 2)
INTERSECT
SELECT tier AS actor
FROM support_agents
ORDER BY actor
