SELECT id, ticket_id AS key_id, actor
FROM ticket_events
UNION ALL
SELECT agent_id AS id, csat AS key_id, priority AS actor
FROM tickets
WHERE TO_DATE(opened_at) < DATE '2025-03-08'
