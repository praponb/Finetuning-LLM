SELECT channel AS category
FROM tickets
INTERSECT
SELECT priority
FROM tickets
WHERE priority IN ('P1', 'P4')
  AND channel IN ('web', 'chat')
ORDER BY category DESC
