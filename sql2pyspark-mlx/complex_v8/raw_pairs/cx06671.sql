SELECT region AS name, holder_name, id
FROM policyholders
WHERE holder_name IS NOT NULL
UNION ALL
SELECT team AS name, adjuster_name AS holder_name, id
FROM adjusters
ORDER BY id
