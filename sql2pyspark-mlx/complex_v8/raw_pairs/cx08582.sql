SELECT id, tier
FROM support_agents
INTERSECT
SELECT requester_id, channel
FROM tickets
ORDER BY id
