SELECT team AS label
FROM support_agents
MINUS
SELECT event_type AS label
FROM ticket_events
WHERE actor IN ('system', 'customer')
  AND ticket_id IS NOT NULL
ORDER BY label DESC
