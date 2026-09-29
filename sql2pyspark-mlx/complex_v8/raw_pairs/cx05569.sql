SELECT id AS qty
FROM policyholders
WHERE joined_on IS NOT NULL
INTERSECT
SELECT level AS qty
FROM adjusters
ORDER BY qty
