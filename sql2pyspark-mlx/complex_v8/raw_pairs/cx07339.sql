SELECT id
FROM support_agents
EXCEPT
SELECT requester_id AS id
FROM tickets
WHERE TO_DATE(opened_at) < DATE '2025-02-06'
