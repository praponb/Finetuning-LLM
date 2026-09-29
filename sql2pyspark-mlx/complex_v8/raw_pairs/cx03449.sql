WITH scoped AS (
    -- Filter adjusters
    SELECT
        id,
        adjuster_name,
        team,
        level,
        SUBSTRING(TRIM(adjuster_name), 1, 3) AS adjuster_name_prefix,
        SUBSTRING(TRIM(adjuster_name), 1, 3) AS adjuster_name_prefix_2
    FROM adjusters
),
latest AS (
    -- Keep the latest row per team
    SELECT
        id,
        adjuster_name,
        team,
        level,
        adjuster_name_prefix,
        adjuster_name_prefix_2
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY team ORDER BY level DESC, id DESC) AS row_num
        FROM scoped
    ) ranked
    WHERE row_num = 1
),
team_stats AS (
    -- Median level per team
    SELECT team, PERCENTILE_APPROX(level, 0.5) AS median_level
    FROM latest
    GROUP BY team
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_level,
        ROUND(s.level - g.median_level, 2) AS level_minus_median
    FROM latest s
    JOIN team_stats g ON g.team = s.team
    WHERE s.level >= g.median_level
)
-- Subtotals with CUBE
SELECT
    team,
    level,
    GROUPING_ID() AS grouping_level,
    ARRAY_SORT(COLLECT_SET(team)) AS team_list
FROM outliers
GROUP BY CUBE(team, level)
