SELECT id
FROM adjusters
WHERE adjuster_name IS NOT NULL
MINUS
SELECT id
FROM adjusters
WHERE team IS NOT NULL
