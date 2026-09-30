SELECT requester_id
FROM tickets
WHERE requester_id <= 16
EXCEPT ALL
SELECT id AS requester_id
FROM support_agents
WHERE tier IS NOT NULL
ORDER BY requester_id
