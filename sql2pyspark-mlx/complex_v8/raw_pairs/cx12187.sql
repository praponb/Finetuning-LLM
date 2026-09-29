SELECT id
FROM policies
INTERSECT
SELECT level AS id
FROM adjusters
WHERE team <> 'Property'
  AND UPPER(TRIM(adjuster_name)) LIKE 'F%'
ORDER BY id DESC
