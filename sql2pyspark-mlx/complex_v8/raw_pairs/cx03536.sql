WITH scoped AS (
    -- Filter adjusters
    SELECT
        id,
        adjuster_name,
        team,
        level,
        COALESCE(level, 0) AS level_filled
    FROM adjusters
    WHERE team IS NOT NULL
      AND LENGTH(TRIM(adjuster_name)) >= 5
),
unioned AS (
    -- Stack the branches
    SELECT
        'adjuster' AS record_type,
        level_filled,
        id,
        adjuster_name
    FROM scoped
    WHERE level < 3
      AND team <> 'Travel'
    UNION ALL
    SELECT
        'policy' AS record_type,
        id,
        holder_id,
        product
    FROM policies
)
SELECT record_type, COUNT(*) AS n_rows, SUM(id) AS total_id
FROM unioned
GROUP BY record_type
ORDER BY record_type
