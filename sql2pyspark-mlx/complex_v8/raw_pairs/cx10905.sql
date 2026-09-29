SELECT csat, requester_id
FROM tickets
WHERE channel IS NOT NULL
  AND closed_at < TIMESTAMP '2025-02-10 04:00:00'
UNION ALL
SELECT requester_id, agent_id
FROM tickets
