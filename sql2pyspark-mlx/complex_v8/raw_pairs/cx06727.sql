SELECT id
FROM ticket_events
MINUS
SELECT agent_id AS id
FROM tickets
ORDER BY id DESC
