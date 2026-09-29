WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        adjuster_name,
        team,
        level,
        ABS(level - 2) AS level_distance
    FROM adjusters
    WHERE LENGTH(TRIM(adjuster_name)) >= 8
)
SELECT adjuster_name, level_distance AS entity_id
FROM valid
WHERE adjuster_name IS NOT NULL
  AND team = 'Health'
UNION ALL
SELECT status, id
FROM policies
ORDER BY entity_id DESC
