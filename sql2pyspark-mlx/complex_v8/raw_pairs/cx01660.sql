SELECT ticket_id AS ref_id, actor AS name, 'event' AS source
FROM ticket_events
UNION ALL
SELECT id, team, 'agent' AS source
FROM support_agents
UNION ALL
SELECT id, agent_name, 'agent' AS source
FROM support_agents
WHERE UPPER(TRIM(agent_name)) LIKE 'L%'
  AND YEAR(hired_on) = 2024
