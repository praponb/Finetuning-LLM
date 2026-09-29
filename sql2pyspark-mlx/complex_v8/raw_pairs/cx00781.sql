SELECT id
FROM tickets
WHERE opened_at < TIMESTAMP '2025-03-02 09:00:00'
  AND priority IN ('P4', 'P2')
INTERSECT ALL
SELECT id
FROM tickets
WHERE agent_id >= 2
  AND csat BETWEEN 1 AND 4
ORDER BY id
