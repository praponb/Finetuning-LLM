SELECT id, ticket_id AS qty
FROM ticket_events
UNION ALL
SELECT ticket_id AS id, id AS qty
FROM ticket_events
ORDER BY id
