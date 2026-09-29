SELECT actor
FROM ticket_events
WHERE event_type IS NOT NULL
  AND HOUR(event_at) BETWEEN 8 AND 10
EXCEPT
SELECT event_type
FROM ticket_events
