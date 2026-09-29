SELECT 'ticket' AS src, agent_id AS qty, priority AS name
FROM tickets
WHERE requester_id > 7
  AND csat < 3
UNION
SELECT 'event' AS src, ticket_id AS qty, actor AS name
FROM ticket_events
WHERE actor = 'system'
ORDER BY name
