WITH base_adjusters AS (
    -- Scope the adjusters rows
    SELECT
        id,
        adjuster_name,
        team,
        level,
        CASE WHEN team IN ('Health', 'Travel') THEN 'group_a' WHEN team = 'Motor' THEN 'group_b' ELSE 'other' END AS team_group
    FROM adjusters
    WHERE team IN ('Health', 'Property', 'Motor')
),
totals AS (
    -- Summarise per group
    SELECT
        team_group,
        level,
        MIN(level) AS min_level,
        MAX(level) AS max_level
    FROM base_adjusters
    GROUP BY 1, 2
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        team_group,
        level,
        min_level,
        max_level,
        NTH_VALUE(level, 2) OVER (ORDER BY min_level, team_group, level ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_level,
        SUM(level) OVER (PARTITION BY max_level) AS max_level_sum_level,
        LEAD(min_level) OVER (ORDER BY min_level, team_group, level) AS next_min_level,
        ROUND(min_level - AVG(min_level) OVER (PARTITION BY max_level), 2) AS min_level_vs_max_level_avg,
        NTH_VALUE(min_level, 2) OVER (ORDER BY min_level, team_group, level ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_min_level
    FROM totals
),
qualified AS (
    -- Compare against the overall min
    SELECT *
    FROM windowed
    WHERE max_level > (SELECT MIN(max_level) FROM windowed)
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT second_level, next_min_level, team_group, min_level_vs_max_level_avg
    FROM qualified
) src
PIVOT (
    SUM(min_level_vs_max_level_avg)
    FOR team_group IN ('group_b', 'other', 'group_a')
)
