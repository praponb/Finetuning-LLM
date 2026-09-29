SELECT agent_id, priority
FROM tickets
WHERE TO_DATE(opened_at) < DATE '2025-02-12'
INTERSECT
SELECT ticket_id, actor
FROM ticket_events
ORDER BY priority DESC, agent_id
