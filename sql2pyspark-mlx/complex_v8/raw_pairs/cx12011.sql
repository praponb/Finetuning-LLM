SELECT id
FROM support_agents
WHERE UPPER(TRIM(agent_name)) LIKE 'F%'
  AND hired_on < DATE '2025-02-27'
EXCEPT
SELECT requester_id
FROM tickets
ORDER BY id DESC
