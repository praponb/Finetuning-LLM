WITH filtered AS (
    -- Scope the adjusters rows
    SELECT
        id,
        adjuster_name,
        team,
        level,
        CONCAT_WS(': ', adjuster_name, team) AS adjuster_name_team_label,
        ABS(level - 3) AS level_distance
    FROM adjusters
    WHERE team IN ('Motor', 'Health', 'Property')
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        id,
        adjuster_name,
        team,
        level,
        adjuster_name_team_label,
        level_distance,
        COUNT(*) OVER (PARTITION BY team) AS rows_in_team,
        ROUND(level - AVG(level) OVER (PARTITION BY level), 2) AS level_vs_level_avg,
        ROUND(AVG(level_distance) OVER (ORDER BY level_distance DESC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS level_distance_avg_6_prior,
        SUM(level_distance) OVER (PARTITION BY level) AS level_sum_level_distance,
        MIN(level) OVER (PARTITION BY team) AS team_min_level,
        ROUND(AVG(level_distance) OVER (ORDER BY level ASC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS level_distance_avg_7,
        SIZE(COLLECT_SET(team) OVER (PARTITION BY level)) AS distinct_team_in_level,
        RANK() OVER (ORDER BY level ASC, id) AS level_rank,
        NTH_VALUE(level, 2) OVER (PARTITION BY team, level ORDER BY level, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_level,
        LAG(level, 1, 0) OVER (ORDER BY level ASC, id) AS prev_level,
        ROUND(level_distance / NULLIF(SUM(level_distance) OVER (PARTITION BY team), 0), 4) AS level_distance_share_of_team
    FROM filtered
),
top_per_group AS (
    -- Top 2 per group
    SELECT *
    FROM ranked
    WHERE level_rank <= 2
)
-- Final projection
SELECT
    id,
    team,
    level,
    adjuster_name_team_label,
    level_distance,
    rows_in_team,
    level_vs_level_avg,
    level_sum_level_distance,
    team_min_level,
    level_distance_avg_7,
    distinct_team_in_level,
    level_rank,
    second_level,
    prev_level,
    level_distance_share_of_team,
    CONCAT(adjuster_name_team_label, '#', CAST(prev_level AS STRING)) AS adjuster_name_team_label_ref
FROM top_per_group
ORDER BY id
