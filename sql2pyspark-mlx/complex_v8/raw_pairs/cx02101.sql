SELECT id
FROM support_agents
WHERE hired_on < DATE '2024-12-31'
  AND tier <> 'T2'
MINUS
SELECT ticket_id AS id
FROM ticket_events
WHERE actor IN ('system', 'customer')
ORDER BY id DESC
