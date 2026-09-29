SELECT csat AS ref_id
FROM tickets
WHERE requester_id < 14
MINUS
SELECT id
FROM tickets
WHERE closed_at < TIMESTAMP '2025-02-20 15:00:00'
  AND csat > 2
