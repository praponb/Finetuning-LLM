SELECT id AS key_id, priority AS label, agent_id AS entity_id
FROM tickets
UNION
SELECT id AS key_id, actor AS label, ticket_id AS entity_id
FROM ticket_events
WHERE MONTH(event_at) IN (1, 3)
ORDER BY entity_id
