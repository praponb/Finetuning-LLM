SELECT adjuster_name, level, team AS name
FROM adjusters
WHERE level <= 3
UNION ALL
SELECT adjuster_name, id, team
FROM adjusters
WHERE level >= 3
  AND UPPER(TRIM(adjuster_name)) LIKE 'E%'
ORDER BY adjuster_name
