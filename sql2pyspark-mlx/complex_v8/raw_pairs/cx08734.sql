SELECT ticket_id, id
FROM ticket_events
WHERE event_type = 'assigned'
UNION
SELECT ticket_id, id
FROM ticket_events
WHERE ticket_id <= 55
  AND event_type IS NOT NULL
ORDER BY ticket_id DESC, id
