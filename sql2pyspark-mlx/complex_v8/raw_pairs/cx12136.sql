WITH base_adjusters AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        adjuster_name,
        team,
        level,
        COALESCE(level, 0) AS level_filled
    FROM adjusters
    WHERE team = 'Travel'
      AND UPPER(TRIM(adjuster_name)) LIKE 'G%'
),
latest AS (
    -- Keep the latest row per team
    SELECT
        id,
        adjuster_name,
        team,
        level,
        level_filled
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY team ORDER BY level DESC, id DESC) AS recency_rank
        FROM base_adjusters
    ) ranked
    WHERE recency_rank = 1
),
qualified AS (
    -- Compare against the overall avg
    SELECT *
    FROM latest
    WHERE level_filled > (SELECT AVG(level_filled) FROM latest)
)
SELECT
    level_filled,
    level,
    COUNT(DISTINCT id) AS distinct_ids,
    ROUND(AVG(level), 2) AS avg_level,
    SUM(level_filled) FILTER (WHERE adjuster_name IS NOT NULL) AS level_filled_adjuster_name_is_not_null
FROM qualified
GROUP BY GROUPING SETS ((level_filled, level), (level_filled), ())
