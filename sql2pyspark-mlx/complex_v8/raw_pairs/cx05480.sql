SELECT priority
FROM tickets
INTERSECT
SELECT actor
FROM ticket_events
WHERE event_type = 'created'
ORDER BY priority
