SELECT id
FROM ticket_events
INTERSECT
SELECT agent_id AS id
FROM tickets
ORDER BY id DESC
