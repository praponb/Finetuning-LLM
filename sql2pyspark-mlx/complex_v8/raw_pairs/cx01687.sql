SELECT event_type AS category, event_at
FROM ticket_events
UNION ALL
SELECT event_type AS category, event_at
FROM ticket_events
WHERE event_at < TIMESTAMP '2025-03-08 08:00:00'
ORDER BY event_at DESC, category
