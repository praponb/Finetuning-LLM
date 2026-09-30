SELECT id
FROM policyholders
EXCEPT
SELECT holder_id
FROM policies
WHERE premium BETWEEN 1042.5 AND 3817.5
  AND start_date >= DATE '2024-06-19'
ORDER BY id
