SELECT id AS key_id
FROM support_agents
WHERE hired_on < DATE '2025-02-27'
  AND team IS NOT NULL
INTERSECT
SELECT id
FROM ticket_events
WHERE event_type = 'escalated'
  AND actor IN ('system', 'agent')
