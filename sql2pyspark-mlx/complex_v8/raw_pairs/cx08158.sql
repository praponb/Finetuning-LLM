SELECT actor, event_at
FROM ticket_events
UNION ALL
SELECT event_type AS actor, event_at
FROM ticket_events
WHERE event_at >= TIMESTAMP '2025-02-06 12:00:00'
