SELECT event_type AS category, actor, event_at
FROM ticket_events
UNION ALL
SELECT priority, channel, closed_at
FROM tickets
ORDER BY event_at, actor
