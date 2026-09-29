SELECT requester_id AS key_id
FROM tickets
WHERE opened_at >= TIMESTAMP '2025-01-19 10:00:00'
EXCEPT
SELECT ticket_id
FROM ticket_events
WHERE HOUR(event_at) BETWEEN 6 AND 9
  AND ticket_id >= 33
ORDER BY key_id
