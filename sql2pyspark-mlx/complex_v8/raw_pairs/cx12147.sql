SELECT ticket_id, event_at
FROM ticket_events
UNION
SELECT requester_id, closed_at
FROM tickets
WHERE priority IS NOT NULL
  AND channel NOT IN ('phone', 'email')
ORDER BY ticket_id DESC
