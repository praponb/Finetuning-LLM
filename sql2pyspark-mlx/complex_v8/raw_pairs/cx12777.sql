SELECT ticket_id
FROM ticket_events
WHERE actor IN ('agent', 'system')
  AND event_at < TIMESTAMP '2025-02-24 12:00:00'
INTERSECT ALL
SELECT requester_id AS ticket_id
FROM tickets
WHERE priority = 'P4'
  AND closed_at >= TIMESTAMP '2025-02-08 15:00:00'
ORDER BY ticket_id
