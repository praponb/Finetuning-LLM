WITH valid_adjusters AS (
    -- Scope the adjusters rows
    SELECT
        id,
        adjuster_name,
        team,
        level
    FROM adjusters
    WHERE LOWER(adjuster_name) LIKE '%g %'
      AND team = 'Property'
)
SELECT team, id
FROM valid_adjusters
WHERE level >= 3
UNION ALL
SELECT team, level
FROM adjusters
WHERE team IS NOT NULL
  AND LOWER(adjuster_name) LIKE '%no%'
UNION ALL
SELECT adjuster_name, id
FROM adjusters
WHERE level <= 3
  AND LENGTH(TRIM(adjuster_name)) >= 5
