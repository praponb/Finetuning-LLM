SELECT ticket_id AS qty
FROM ticket_events
EXCEPT
SELECT id
FROM support_agents
WHERE tier NOT IN ('T3', 'T2')
  AND LENGTH(TRIM(agent_name)) >= 6
