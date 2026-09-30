SELECT level
FROM adjusters
EXCEPT
SELECT id AS level
FROM policyholders
WHERE UPPER(TRIM(holder_name)) LIKE 'N%'
  AND region = 'Central'
ORDER BY level DESC
