WITH filtered_adjusters AS (
    -- Filter adjusters
    SELECT
        id,
        adjuster_name,
        team,
        level,
        CONCAT(team, '#', CAST(id AS STRING)) AS team_ref,
        CASE WHEN level >= 3 THEN 'A' WHEN level >= 2 THEN 'B' ELSE 'C' END AS level_tier,
        COALESCE(level, 0) AS level_filled
    FROM adjusters
    WHERE LOWER(adjuster_name) LIKE '%fa%'
),
per_group AS (
    -- Aggregate with conditional metrics
    SELECT
        team,
        SUM(level_filled) FILTER (WHERE team NOT IN ('Property', 'Travel')) AS level_filled_team_not_in_property_travel,
        COUNT(*) FILTER (WHERE LENGTH(TRIM(adjuster_name)) >= 7) AS n_length_trim_adjuster_name_7,
        MIN(level) AS min_level,
        COUNT(*) FILTER (WHERE LENGTH(TRIM(adjuster_name)) >= 8) AS n_length_trim_adjuster_name_8,
        ARRAY_SORT(COLLECT_SET(adjuster_name)) AS adjuster_name_list
    FROM filtered_adjusters
    GROUP BY team
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        FIRST_VALUE(min_level) OVER (PARTITION BY n_length_trim_adjuster_name_7 ORDER BY level_filled_team_not_in_property_travel NULLS LAST, team ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_min_level,
        COUNT(*) OVER (PARTITION BY n_length_trim_adjuster_name_7 ORDER BY level_filled_team_not_in_property_travel NULLS LAST, team ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        NTH_VALUE(min_level, 2) OVER (ORDER BY n_length_trim_adjuster_name_7, team ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_min_level,
        min_level - LAG(min_level) OVER w_n_length_trim_adjuster_name_3 AS min_level_change
    FROM per_group
    WINDOW w_n_length_trim_adjuster_name_3 AS (PARTITION BY n_length_trim_adjuster_name_8 ORDER BY n_length_trim_adjuster_name_7 NULLS LAST, team)
),
metrics_2 AS (
    -- Rank and compare rows within groups
    SELECT
        team,
        level_filled_team_not_in_property_travel,
        n_length_trim_adjuster_name_7,
        min_level,
        adjuster_name_list,
        running_count,
        second_min_level,
        min_level_change,
        ROUND(AVG(min_level) OVER (PARTITION BY min_level), 2) AS min_level_avg_min_level,
        ROUND(PERCENT_RANK() OVER w_team_min_level_by_min_level, 3) AS min_level_percent_rank,
        SIZE(COLLECT_SET(team) OVER (PARTITION BY min_level)) AS distinct_team_in_min_level,
        ROUND(CUME_DIST() OVER (PARTITION BY team, min_level ORDER BY min_level, team), 3) AS min_level_cume_dist,
        MAX(first_min_level) OVER (PARTITION BY min_level) AS min_level_max_first_min_level
    FROM metrics
    WINDOW w_team_min_level_by_min_level AS (PARTITION BY team, min_level ORDER BY min_level)
)
SELECT *
FROM (
    SELECT min_level_change, n_length_trim_adjuster_name_7, team, running_count
    FROM metrics_2
) src
PIVOT (
    AVG(running_count)
    FOR team IN ('Health', 'Property', 'Travel', 'Motor')
)
ORDER BY min_level_change, n_length_trim_adjuster_name_7
