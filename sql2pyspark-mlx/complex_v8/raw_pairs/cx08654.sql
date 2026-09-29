SELECT id, tier
FROM support_agents
INTERSECT
SELECT requester_id AS id, channel AS tier
FROM tickets
