SELECT id, team AS name
FROM adjusters
WHERE level BETWEEN 1 AND 3
  AND UPPER(TRIM(adjuster_name)) LIKE 'F%'
INTERSECT ALL
SELECT id, team AS name
FROM adjusters
WHERE team <> 'Property'
