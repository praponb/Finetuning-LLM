SELECT id
FROM support_agents
WHERE UPPER(TRIM(agent_name)) LIKE 'H%'
EXCEPT
SELECT csat
FROM tickets
WHERE requester_id <= 19
ORDER BY id DESC
