SELECT csat AS key_id, requester_id AS entity_id
FROM tickets
WHERE channel IS NOT NULL
  AND requester_id <= 18
UNION ALL
SELECT agent_id AS key_id, requester_id AS entity_id
FROM tickets
