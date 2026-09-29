SELECT agent_name
FROM support_agents
WHERE tier = 'T2'
EXCEPT
SELECT channel AS agent_name
FROM tickets
WHERE requester_id <= 23
  AND channel = 'web'
ORDER BY agent_name
