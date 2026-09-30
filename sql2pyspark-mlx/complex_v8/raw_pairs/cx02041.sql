SELECT ticket_id AS ref_id
FROM ticket_events
EXCEPT ALL
SELECT agent_id
FROM tickets
ORDER BY ref_id DESC
