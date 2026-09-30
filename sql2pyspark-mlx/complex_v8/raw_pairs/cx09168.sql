SELECT ticket_id
FROM ticket_events
WHERE event_type <> 'assigned'
INTERSECT ALL
SELECT id AS ticket_id
FROM ticket_events
WHERE ticket_id <= 11
  AND event_at < TIMESTAMP '2025-02-24 13:00:00'
