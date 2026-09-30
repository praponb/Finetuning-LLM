WITH cleaned_adjusters AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        adjuster_name,
        team,
        level,
        ABS(level - 2) AS level_distance
    FROM adjusters
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        id,
        adjuster_name,
        team,
        level,
        level_distance,
        LAST_VALUE(level) OVER (ORDER BY level, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_level,
        SIZE(COLLECT_SET(team) OVER w_level_distance) AS distinct_team_in_level_distance,
        ROUND(level - AVG(level) OVER (PARTITION BY level_distance), 2) AS level_vs_level_distance_avg,
        ROUND(level - AVG(level) OVER (PARTITION BY level_distance, team), 2) AS level_vs_level_distance_team_avg,
        COUNT(*) OVER (ORDER BY level DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        ROUND(level / NULLIF(SUM(level) OVER (PARTITION BY level_distance, team), 0), 4) AS level_share_of_level_distance_team,
        LEAD(level, 3, 0) OVER (PARTITION BY team, level ORDER BY level_distance, id) AS next3_level,
        ROUND(AVG(level) OVER (PARTITION BY level_distance, team ORDER BY level DESC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS level_avg_1_prior,
        level - LAG(level) OVER w_all_by_level AS level_change,
        ROUND(PERCENT_RANK() OVER (ORDER BY level), 3) AS level_percent_rank
    FROM cleaned_adjusters
    WINDOW w_level_distance AS (PARTITION BY level_distance), w_all_by_level AS (ORDER BY level DESC, id)
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        SIZE(COLLECT_SET(team) OVER (PARTITION BY level_distance)) AS distinct_team_in_level_distance_2,
        MIN(level_distance) OVER (PARTITION BY level_distance) AS level_distance_min_level_distance,
        SUM(level_percent_rank) OVER (PARTITION BY last_level ORDER BY level_distance DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS level_percent_rank_sum_2_prior,
        MAX(level_vs_level_distance_avg) OVER (PARTITION BY level_distance ORDER BY level_share_of_level_distance_team ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_level_vs_level_distance_avg_so_far,
        ROUND(level / NULLIF(SUM(level) OVER (PARTITION BY level_distance), 0), 4) AS level_share_of_level_distance,
        NTH_VALUE(level_avg_1_prior, 2) OVER (PARTITION BY last_level ORDER BY level_distance DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_level_avg_1_prior,
        ROUND(CUME_DIST() OVER w_level_distance_by_level_shar_2, 3) AS level_share_of_level_distance_team_cume_dist
    FROM with_trends
    WINDOW w_level_distance_by_level_shar_2 AS (PARTITION BY level_distance ORDER BY level_share_of_level_distance_team ASC, id)
)
-- Final projection
SELECT
    id,
    adjuster_name,
    level,
    level_distance,
    level_vs_level_distance_avg,
    level_vs_level_distance_team_avg,
    running_count,
    level_share_of_level_distance_team,
    next3_level,
    level_percent_rank,
    distinct_team_in_level_distance_2,
    max_level_vs_level_distance_avg_so_far,
    level_share_of_level_distance,
    CASE WHEN level_avg_1_prior >= 3.2 THEN 'A' WHEN level_avg_1_prior >= 1.8 THEN 'B' ELSE 'C' END AS level_avg_1_prior_tier
FROM metrics
ORDER BY level_vs_level_distance_avg DESC, distinct_team_in_level_distance_2
