SELECT id AS qty, level
FROM adjusters
UNION ALL
SELECT id, level
FROM adjusters
WHERE level < 3
  AND adjuster_name IS NOT NULL
