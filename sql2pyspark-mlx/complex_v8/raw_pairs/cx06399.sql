WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        adjuster_name,
        team,
        level,
        COALESCE(level, 0) AS level_filled
    FROM adjusters
    WHERE LENGTH(TRIM(adjuster_name)) >= 7
      AND level <= 3
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        SIZE(COLLECT_SET(team) OVER (PARTITION BY level_filled, level)) AS distinct_team_in_level_filled_level,
        DENSE_RANK() OVER (PARTITION BY team, level_filled ORDER BY level_filled DESC) AS level_filled_dense_rank,
        SUM(level_filled) OVER (PARTITION BY level_filled, level) AS level_filled_level_sum_level_filled,
        MAX(level_filled) OVER (PARTITION BY level_filled, level) AS level_filled_level_max_level_filled,
        LEAD(level) OVER (PARTITION BY team, level_filled ORDER BY level_filled DESC, id) AS next_level,
        ROUND(level_filled - AVG(level_filled) OVER (PARTITION BY level_filled, level), 2) AS level_filled_vs_level_filled_level_avg,
        NTILE(10) OVER (PARTITION BY team, level_filled ORDER BY level_filled DESC, id) AS level_filled_decile,
        COUNT(*) OVER (PARTITION BY level_filled, level) AS rows_in_level_filled_level,
        level_filled - LAG(level_filled) OVER w_team_level_filled_by_level_f_2 AS level_filled_change
    FROM filtered
    WINDOW w_team_level_filled_by_level_f_2 AS (PARTITION BY team, level_filled ORDER BY level_filled DESC, id), w_level_filled_level AS (PARTITION BY level_filled, level)
),
leaders AS (
    -- First row per group
    SELECT
        id,
        adjuster_name,
        team,
        level,
        level_filled,
        distinct_team_in_level_filled_level,
        level_filled_level_sum_level_filled,
        level_filled_level_max_level_filled,
        next_level,
        level_filled_vs_level_filled_level_avg,
        level_filled_decile,
        rows_in_level_filled_level,
        level_filled_change
    FROM windowed
    WHERE level_filled_dense_rank = 1
      AND level_filled_change <= 2
)
SELECT
    id,
    level,
    level_filled,
    distinct_team_in_level_filled_level,
    level_filled_level_sum_level_filled,
    level_filled_level_max_level_filled,
    next_level,
    level_filled_vs_level_filled_level_avg,
    level_filled_decile,
    level_filled_change
FROM leaders
ORDER BY id DESC
