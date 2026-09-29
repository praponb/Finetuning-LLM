WITH cleaned_adjusters AS (
    -- Scope the adjusters rows
    SELECT
        id,
        adjuster_name,
        team,
        level
    FROM adjusters
    WHERE (UPPER(TRIM(adjuster_name)) LIKE 'H%' OR level >= 1)
),
scored AS (
    -- Add calculated columns
    SELECT
        *,
        CONCAT(adjuster_name, '#', CAST(level AS STRING)) AS adjuster_name_ref
    FROM cleaned_adjusters
    WHERE team <> 'Health'
),
per_group AS (
    -- Aggregate with conditional metrics
    SELECT
        level,
        SUM(level) AS total_level,
        COUNT(*) AS n_rows,
        MIN(level) AS min_level
    FROM scored
    GROUP BY level
)
-- Final projection
SELECT
    level,
    total_level,
    n_rows,
    min_level,
    ROUND(level / NULLIF(n_rows, 0), 3) AS level_per_n_rows
FROM per_group
ORDER BY level DESC NULLS FIRST
