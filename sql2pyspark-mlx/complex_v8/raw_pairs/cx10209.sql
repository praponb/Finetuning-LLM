WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        adjuster_name,
        team,
        level
    FROM adjusters
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        LEAD(level, 3) OVER (PARTITION BY level, team ORDER BY level, id) AS next3_level,
        SUM(level) OVER (PARTITION BY team ORDER BY level NULLS LAST, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS level_sum_4,
        ROW_NUMBER() OVER (PARTITION BY team ORDER BY level ASC, id) AS seq_no,
        FIRST_VALUE(level) IGNORE NULLS OVER (PARTITION BY level, team ORDER BY level, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_level,
        level - LAG(level) OVER (PARTITION BY team ORDER BY level ASC, id) AS level_change,
        ROUND(level / NULLIF(SUM(level) OVER (PARTITION BY level), 0), 4) AS level_share_of_level,
        ROUND(level - AVG(level) OVER (PARTITION BY level), 2) AS level_vs_level_avg,
        NTH_VALUE(level, 2) OVER (PARTITION BY team ORDER BY level NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_level,
        MAX(level) OVER (PARTITION BY level) AS level_max_level,
        NTILE(10) OVER (PARTITION BY team ORDER BY level NULLS LAST, id) AS level_decile,
        MIN(level) OVER (PARTITION BY level) AS level_min_level,
        ROW_NUMBER() OVER (PARTITION BY team ORDER BY level NULLS LAST, id) AS seq_no_2,
        ROUND(CUME_DIST() OVER (PARTITION BY team ORDER BY level ASC), 3) AS level_cume_dist
    FROM cleaned
),
windowed_2 AS (
    -- Running and moving aggregates
    SELECT
        id,
        adjuster_name,
        team,
        seq_no,
        level_change,
        level_share_of_level,
        level_vs_level_avg,
        second_level,
        level_max_level,
        level_min_level,
        seq_no_2,
        level_cume_dist,
        DENSE_RANK() OVER (PARTITION BY level ORDER BY level_decile) AS level_decile_dense_rank,
        NTILE(3) OVER (PARTITION BY level, level_change ORDER BY level_max_level, id) AS level_max_level_tercile,
        ROUND(CUME_DIST() OVER w_level_level_change_by_level, 3) AS level_max_level_cume_dist,
        DENSE_RANK() OVER w_level_level_change_by_level_2 AS level_max_level_dense_rank,
        ROUND(seq_no_2 - AVG(seq_no_2) OVER w_first_known_level, 2) AS seq_no_2_vs_first_known_level_avg,
        FIRST_VALUE(level) OVER (PARTITION BY level, level_change ORDER BY level_max_level, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_level,
        DENSE_RANK() OVER w_level_decile_by_level_vs_lev AS level_vs_level_avg_dense_rank
    FROM windowed
    WINDOW w_level_level_change_by_level AS (PARTITION BY level, level_change ORDER BY level_max_level, id), w_level_level_change_by_level_2 AS (PARTITION BY level, level_change ORDER BY level_max_level), w_first_known_level AS (PARTITION BY first_known_level), w_level_decile_by_level_vs_lev AS (PARTITION BY level_decile ORDER BY level_vs_level_avg, id)
)
-- Final projection
SELECT
    id,
    adjuster_name,
    team,
    seq_no,
    level_change,
    second_level,
    level_max_level,
    level_min_level,
    seq_no_2,
    level_decile_dense_rank,
    level_max_level_tercile,
    level_max_level_cume_dist,
    seq_no_2_vs_first_known_level_avg,
    first_level
FROM windowed_2
ORDER BY team DESC NULLS FIRST
