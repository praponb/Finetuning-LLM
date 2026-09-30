SELECT agent_id AS key_id, channel
FROM tickets
EXCEPT
SELECT id AS key_id, actor AS channel
FROM ticket_events
ORDER BY channel DESC, key_id
