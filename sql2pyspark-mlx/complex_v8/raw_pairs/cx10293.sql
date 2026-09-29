SELECT actor
FROM ticket_events
WHERE event_type IN ('replied', 'escalated')
  AND ticket_id <= 11
INTERSECT ALL
SELECT agent_name
FROM support_agents
WHERE team IN ('Tech', 'VIP')
  AND LENGTH(TRIM(agent_name)) >= 4
