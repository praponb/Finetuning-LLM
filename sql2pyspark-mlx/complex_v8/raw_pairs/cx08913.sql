SELECT ticket_id AS key_id
FROM ticket_events
WHERE actor IS NOT NULL
  AND ticket_id IS NOT NULL
EXCEPT
SELECT id AS key_id
FROM support_agents
WHERE UPPER(TRIM(agent_name)) LIKE 'I%'
