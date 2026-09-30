SELECT requester_id
FROM tickets
WHERE priority = 'P2'
EXCEPT
SELECT id AS requester_id
FROM support_agents
WHERE hired_on < DATE '2025-02-26'
ORDER BY requester_id DESC
