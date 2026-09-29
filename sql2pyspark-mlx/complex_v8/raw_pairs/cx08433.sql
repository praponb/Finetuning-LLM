SELECT agent_id
FROM tickets
WHERE channel <> 'phone'
INTERSECT
SELECT agent_id
FROM tickets
WHERE requester_id <= 19
ORDER BY agent_id
