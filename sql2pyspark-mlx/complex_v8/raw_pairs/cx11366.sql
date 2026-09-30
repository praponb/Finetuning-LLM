SELECT requester_id, channel AS label
FROM tickets
INTERSECT
SELECT csat AS requester_id, priority AS label
FROM tickets
WHERE agent_id IS NOT NULL
  AND closed_at >= TIMESTAMP '2025-01-20 22:00:00'
ORDER BY requester_id DESC
