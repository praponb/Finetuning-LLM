SELECT id AS key_id
FROM tickets
WHERE priority <> 'P2'
  AND channel IN ('chat', 'email')
MINUS
SELECT id
FROM ticket_events
WHERE ticket_id IS NOT NULL
  AND actor = 'system'
