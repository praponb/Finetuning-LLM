SELECT status
FROM claims
EXCEPT
SELECT status
FROM policies
WHERE premium BETWEEN 918.1 AND 3532.7
  AND start_date < DATE '2025-05-30'
ORDER BY status DESC
