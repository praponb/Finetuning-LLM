SELECT 'event' AS origin, actor AS name, event_type
FROM ticket_events
WHERE event_type = 'escalated'
  AND ticket_id IS NOT NULL
UNION ALL
SELECT 'agent' AS origin, team, agent_name
FROM support_agents
WHERE hired_on BETWEEN DATE '2024-07-09' AND DATE '2025-03-30'
ORDER BY origin DESC, name
