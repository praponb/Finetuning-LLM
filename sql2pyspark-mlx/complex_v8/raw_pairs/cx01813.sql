SELECT agent_id
FROM tickets
EXCEPT ALL
SELECT ticket_id
FROM ticket_events
WHERE actor NOT IN ('agent', 'system')
ORDER BY agent_id
