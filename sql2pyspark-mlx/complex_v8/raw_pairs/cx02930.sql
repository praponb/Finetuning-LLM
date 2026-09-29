WITH prepared_adjusters AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        adjuster_name,
        team,
        level
    FROM adjusters
    WHERE team = 'Travel'
      AND level > 2
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAG(level, 1, 0) OVER (PARTITION BY level, team ORDER BY level, id) AS prev_level,
        NTILE(10) OVER (PARTITION BY level ORDER BY level ASC, id) AS level_decile,
        ROUND(CUME_DIST() OVER (PARTITION BY level ORDER BY level ASC, id), 3) AS level_cume_dist,
        ROUND(PERCENT_RANK() OVER (PARTITION BY level, team ORDER BY level), 3) AS level_percent_rank,
        FIRST_VALUE(level) IGNORE NULLS OVER (PARTITION BY level ORDER BY level ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_level
    FROM prepared_adjusters
)
SELECT
    level,
    level_decile,
    ARRAY_SORT(COLLECT_SET(adjuster_name)) AS adjuster_name_list
FROM with_windows
GROUP BY CUBE(level, level_decile)
ORDER BY level NULLS LAST, level_decile NULLS LAST
