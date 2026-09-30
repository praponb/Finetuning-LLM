SELECT csat AS key_id, channel AS label
FROM tickets
WHERE requester_id >= 8
  AND closed_at < TIMESTAMP '2025-02-26 16:00:00'
UNION
SELECT id, team
FROM support_agents
ORDER BY label
