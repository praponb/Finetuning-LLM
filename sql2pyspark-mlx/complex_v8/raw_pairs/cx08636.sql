SELECT requester_id, channel AS category
FROM tickets
WHERE agent_id IS NOT NULL
  AND channel IN ('chat', 'phone', 'email')
INTERSECT ALL
SELECT ticket_id AS requester_id, actor AS category
FROM ticket_events
WHERE event_type = 'escalated'
  AND actor <> 'customer'
ORDER BY category
