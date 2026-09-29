SELECT id AS qty
FROM policyholders
EXCEPT
SELECT id AS qty
FROM adjusters
WHERE UPPER(TRIM(adjuster_name)) LIKE 'B%'
ORDER BY qty
