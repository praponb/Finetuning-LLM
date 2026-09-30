SELECT id AS ref_id, actor
FROM ticket_events
WHERE event_type = 'assigned'
  AND MONTH(event_at) IN (2, 3)
INTERSECT ALL
SELECT id, agent_name
FROM support_agents
