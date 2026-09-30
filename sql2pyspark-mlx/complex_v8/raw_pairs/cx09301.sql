SELECT level
FROM adjusters
INTERSECT
SELECT adjuster_id AS level
FROM claims
ORDER BY level DESC
