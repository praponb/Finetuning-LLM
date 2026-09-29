SELECT event_type AS label, id, 'event' AS src
FROM ticket_events
UNION ALL
SELECT agent_name AS label, id, 'agent' AS src
FROM support_agents
WHERE YEAR(hired_on) = 2025
