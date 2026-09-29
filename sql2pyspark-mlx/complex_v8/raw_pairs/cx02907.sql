WITH prepared AS (
    -- Filter adjusters
    SELECT
        id,
        adjuster_name,
        team,
        level
    FROM adjusters
    WHERE UPPER(TRIM(adjuster_name)) LIKE 'G%'
),
labeled AS (
    -- Derive helper columns
    SELECT *, ABS(level - 3) AS level_distance
    FROM prepared
),
per_group AS (
    -- Summarise per group
    SELECT
        team,
        level,
        COUNT(*) AS row_count,
        ARRAY_SORT(COLLECT_SET(team)) AS team_list,
        COUNT(DISTINCT id) AS distinct_ids
    FROM labeled
    GROUP BY 1, 2
),
qualified AS (
    -- Compare against the overall avg
    SELECT *
    FROM per_group
    WHERE level > (SELECT AVG(level) FROM per_group)
)
-- Native SQL UNPIVOT (wide to long)
SELECT team, level, field, reading
FROM (
    SELECT team, level, distinct_ids, row_count
    FROM qualified
) src
UNPIVOT (
    reading FOR field IN (distinct_ids, row_count)
) unp
ORDER BY team, level, field
