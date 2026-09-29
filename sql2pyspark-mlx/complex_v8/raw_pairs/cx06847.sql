SELECT 'ticket' AS src, id, csat
FROM tickets
WHERE channel = 'chat'
UNION ALL
SELECT 'event' AS src, id, ticket_id
FROM ticket_events
UNION ALL
SELECT 'ticket_3' AS src, id, agent_id
FROM tickets
