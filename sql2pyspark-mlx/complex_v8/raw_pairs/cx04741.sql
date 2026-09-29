SELECT csat
FROM tickets
INTERSECT
SELECT id
FROM ticket_events
WHERE ticket_id >= 37
  AND event_at < TIMESTAMP '2025-03-08 09:00:00'
