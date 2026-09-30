SELECT ticket_id AS qty, event_type
FROM ticket_events
WHERE ticket_id IS NOT NULL
  AND event_type IS NOT NULL
INTERSECT ALL
SELECT id, agent_name
FROM support_agents
