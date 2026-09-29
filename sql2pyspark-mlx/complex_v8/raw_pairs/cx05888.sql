WITH filtered AS (
    -- Scope the adjusters rows
    SELECT
        id,
        adjuster_name,
        team,
        level,
        COALESCE(level, 0) AS level_filled
    FROM adjusters
    WHERE level BETWEEN 2 AND 3
      AND UPPER(TRIM(adjuster_name)) LIKE 'E%'
)
SELECT id, level AS key_id
FROM filtered
UNION
SELECT holder_id, id
FROM policies
WHERE holder_id IS NOT NULL
