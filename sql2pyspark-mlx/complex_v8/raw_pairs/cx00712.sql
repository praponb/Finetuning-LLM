SELECT id
FROM tickets
WHERE closed_at >= TIMESTAMP '2025-01-17 21:00:00'
  AND MONTH(opened_at) IN (1, 2)
EXCEPT
SELECT id
FROM tickets
WHERE agent_id >= 6
  AND closed_at < TIMESTAMP '2025-02-15 09:00:00'
