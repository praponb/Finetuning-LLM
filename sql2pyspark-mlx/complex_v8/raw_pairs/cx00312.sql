SELECT id AS qty
FROM support_agents
WHERE hired_on >= DATE '2024-07-09'
  AND UPPER(TRIM(agent_name)) LIKE 'H%'
INTERSECT
SELECT id AS qty
FROM ticket_events
ORDER BY qty
