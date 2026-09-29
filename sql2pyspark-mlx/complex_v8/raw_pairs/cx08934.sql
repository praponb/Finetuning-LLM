WITH cleaned AS (
    -- Scope the adjusters rows
    SELECT
        id,
        adjuster_name,
        team,
        level
    FROM adjusters
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(level / NULLIF(SUM(level) OVER (PARTITION BY level), 0), 4) AS level_share_of_level,
        NTH_VALUE(level, 2) OVER (PARTITION BY level, team ORDER BY level ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_level,
        LEAD(level, 2) OVER (PARTITION BY team, level ORDER BY level, id) AS next2_level,
        RANK() OVER w_team_level_by_level_ties AS level_rank,
        LAST_VALUE(level) IGNORE NULLS OVER (PARTITION BY team, level ORDER BY level, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_level,
        MAX(level) OVER (PARTITION BY team) AS team_max_level,
        LAG(level, 2, 0) OVER (PARTITION BY team, level ORDER BY level, id) AS prev2_level,
        NTILE(10) OVER w_team_level_by_level AS level_decile
    FROM cleaned
    WINDOW w_team_level_by_level_ties AS (PARTITION BY team, level ORDER BY level DESC), w_team AS (PARTITION BY team), w_team_level_by_level AS (PARTITION BY team, level ORDER BY level, id)
),
annotated AS (
    -- Derive helper columns
    SELECT
        *,
        ABS(level_rank - 4) AS level_rank_distance,
        LEAST(level_share_of_level, level_rank) AS least_level_share_of_level_level_rank
    FROM windowed
    WHERE next2_level BETWEEN 1 AND 3
),
windowed_2 AS (
    -- Window metrics over several partitions
    SELECT
        id,
        level,
        level_share_of_level,
        second_level,
        team_max_level,
        prev2_level,
        level_decile,
        level_rank_distance,
        least_level_share_of_level_level_rank,
        SUM(next2_level) OVER (PARTITION BY last_known_level, prev2_level) AS last_known_level_prev2_level_sum_next2_level,
        ROUND(CUME_DIST() OVER (PARTITION BY team ORDER BY team_max_level DESC NULLS FIRST), 3) AS team_max_level_cume_dist,
        MAX(prev2_level) OVER (ORDER BY prev2_level DESC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS prev2_level_max_2,
        NTILE(3) OVER (PARTITION BY level ORDER BY last_known_level DESC, id) AS last_known_level_tercile,
        NTH_VALUE(next2_level, 2) OVER (ORDER BY team_max_level DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_next2_level,
        FIRST_VALUE(level_rank) OVER (ORDER BY team_max_level DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_level_rank
    FROM annotated
),
rollup_base AS (
    -- Group-level totals
    SELECT
        level_decile,
        ROUND(STDDEV(second_next2_level), 2) AS stddev_second_next2_level,
        COUNT(DISTINCT id) AS distinct_ids,
        COUNT(*) AS n_rows,
        PERCENTILE_APPROX(prev2_level, 0.5) AS median_prev2_level,
        ROUND(SUM(last_known_level_tercile) / NULLIF(SUM(last_known_level_prev2_level_sum_next2_level), 0), 3) AS last_known_level_tercile_to_last_known_level_pre
    FROM windowed_2
    GROUP BY level_decile
)
-- Final projection
SELECT
    level_decile,
    stddev_second_next2_level,
    distinct_ids,
    LEAST(median_prev2_level, level_decile) AS least_median_prev2_level_level_decile,
    GREATEST(distinct_ids, n_rows) AS greatest_distinct_ids_n_rows
FROM rollup_base
WHERE median_prev2_level >= 2
