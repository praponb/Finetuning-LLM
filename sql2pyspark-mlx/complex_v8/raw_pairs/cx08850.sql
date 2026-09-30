SELECT ticket_id AS qty, event_type
FROM ticket_events
WHERE event_type IN ('created', 'assigned')
  AND event_at < TIMESTAMP '2025-02-12 09:00:00'
INTERSECT
SELECT id AS qty, agent_name AS event_type
FROM support_agents
WHERE team = 'VIP'
ORDER BY qty DESC, event_type
