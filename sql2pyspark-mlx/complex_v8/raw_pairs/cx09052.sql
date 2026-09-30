SELECT adjuster_name AS label
FROM adjusters
WHERE level > 2
  AND UPPER(TRIM(adjuster_name)) LIKE 'A%'
INTERSECT
SELECT adjuster_name AS label
FROM adjusters
WHERE LOWER(adjuster_name) LIKE '%rt%'
  AND level < 3
ORDER BY label DESC
