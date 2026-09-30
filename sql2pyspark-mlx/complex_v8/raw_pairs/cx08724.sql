SELECT id AS qty
FROM support_agents
WHERE tier <> 'T3'
  AND team <> 'Tech'
EXCEPT
SELECT ticket_id
FROM ticket_events
WHERE actor = 'customer'
ORDER BY qty DESC
