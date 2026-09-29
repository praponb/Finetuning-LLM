WITH valid AS (
    -- Filter adjusters
    SELECT
        id,
        adjuster_name,
        team,
        level
    FROM adjusters
),
totals AS (
    -- Group-level totals
    SELECT
        level,
        team,
        ARRAY_SORT(COLLECT_SET(adjuster_name)) AS adjuster_name_list,
        ARRAY_SORT(COLLECT_SET(team)) AS team_list,
        PERCENTILE_APPROX(level, 0.5) AS median_level,
        MAX(level) AS max_level
    FROM valid
    WHERE team IN ('Property', 'Travel', 'Health')
    GROUP BY 1, 2
    HAVING AVG(level) > 2
),
scored AS (
    -- Label and score rows
    SELECT
        *,
        CASE WHEN team IN ('Property', 'Health') THEN 'group_a' WHEN team = 'Travel' THEN 'group_b' ELSE 'other' END AS team_group
    FROM totals
    WHERE max_level <= 3
),
screened AS (
    -- Compare against the overall avg
    SELECT *
    FROM scored
    WHERE max_level > (SELECT AVG(max_level) FROM scored)
),
metrics AS (
    -- Previous / next values per key
    SELECT
        level,
        team,
        median_level,
        max_level,
        COUNT(*) OVER (PARTITION BY level, median_level) AS rows_in_level_median_level,
        MAX(level) OVER w_level_median_level AS level_median_level_max_level,
        SIZE(COLLECT_SET(team_group) OVER (PARTITION BY level, median_level)) AS distinct_team_group_in_level_median_level
    FROM screened
    WINDOW w_level_median_level AS (PARTITION BY level, median_level)
)
-- Final projection
SELECT
    level,
    team,
    median_level,
    max_level,
    distinct_team_group_in_level_median_level,
    ABS(max_level - 3) AS max_level_distance,
    COALESCE(level, 0) AS level_filled
FROM metrics
WHERE rows_in_level_median_level > 7
ORDER BY median_level NULLS LAST, distinct_team_group_in_level_median_level DESC
