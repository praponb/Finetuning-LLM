SELECT level AS key_id
FROM adjusters
WHERE team = 'Travel'
  AND LENGTH(TRIM(adjuster_name)) >= 6
EXCEPT
SELECT holder_id
FROM policies
WHERE status = 'LAPSED'
  AND start_date BETWEEN DATE '2024-07-19' AND DATE '2025-08-03'
ORDER BY key_id
