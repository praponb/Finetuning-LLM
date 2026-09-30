WITH valid_adjusters AS (
    -- Filter adjusters
    SELECT *
    FROM adjusters
    WHERE LENGTH(TRIM(adjuster_name)) >= 4
      AND team = 'Health'
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        LAST_VALUE(level) IGNORE NULLS OVER (PARTITION BY team, level ORDER BY level DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_level,
        LAST_VALUE(level) IGNORE NULLS OVER (ORDER BY level DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_level_2,
        FIRST_VALUE(level) OVER (PARTITION BY team, level ORDER BY level DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_level,
        COUNT(*) OVER w_team_level AS rows_in_team_level,
        ROUND(level / NULLIF(SUM(level) OVER (PARTITION BY team), 0), 4) AS level_share_of_team,
        ROUND(PERCENT_RANK() OVER (PARTITION BY team, level ORDER BY level, id), 3) AS level_percent_rank,
        NTH_VALUE(level, 2) OVER (ORDER BY level DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_level,
        NTILE(3) OVER (PARTITION BY team, level ORDER BY level, id) AS level_tercile,
        ROUND(level - AVG(level) OVER w_team, 2) AS level_vs_team_avg,
        level - LAG(level) OVER (ORDER BY level ASC NULLS LAST, id) AS level_change,
        NTH_VALUE(level, 2) OVER (PARTITION BY team, level ORDER BY level, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_level_2,
        LEAD(level, 2) OVER w_all_by_level AS next2_level,
        RANK() OVER w_level_team_by_level_ties AS level_rank,
        ROUND(level - AVG(level) OVER w_level, 2) AS level_vs_level_avg,
        ROUND(PERCENT_RANK() OVER (PARTITION BY level, team ORDER BY level, id), 3) AS level_percent_rank_2
    FROM valid_adjusters
    WINDOW w_team_level AS (PARTITION BY team, level), w_team AS (PARTITION BY team), w_all_by_level AS (ORDER BY level ASC NULLS LAST, id), w_level_team_by_level_ties AS (PARTITION BY level, team ORDER BY level), w_level AS (PARTITION BY level)
),
top_rows AS (
    -- First row per group
    SELECT
        id,
        adjuster_name,
        team,
        level,
        last_known_level,
        last_known_level_2,
        first_level,
        rows_in_team_level,
        level_share_of_team,
        level_percent_rank,
        second_level,
        level_tercile,
        level_vs_team_avg,
        level_change,
        second_level_2,
        next2_level,
        level_vs_level_avg,
        level_percent_rank_2
    FROM with_windows
    WHERE level_rank = 1
      AND level_vs_team_avg >= -0.5
),
with_windows_2 AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(level_tercile / NULLIF(SUM(level_tercile) OVER (PARTITION BY last_known_level_2, next2_level), 0), 4) AS level_tercile_share_of_last_known_level_2_next2_,
        COUNT(*) OVER (PARTITION BY last_known_level_2, next2_level) AS rows_in_last_known_level_2_next2_level,
        SUM(second_level) OVER (PARTITION BY level, last_known_level) AS level_last_known_level_sum_second_level,
        ROUND(AVG(level_tercile) OVER (ORDER BY level_vs_level_avg, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS level_tercile_avg_2_prior,
        SUM(next2_level) OVER (PARTITION BY last_known_level, last_known_level_2 ORDER BY level_percent_rank, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS next2_level_sum_1_prior,
        LAST_VALUE(level_share_of_team) IGNORE NULLS OVER (ORDER BY level_vs_level_avg, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_level_share_of_team,
        ROUND(AVG(level_tercile) OVER w_level_last_known_level, 2) AS level_last_known_level_avg_level_tercile
    FROM top_rows
    WINDOW w_level_last_known_level AS (PARTITION BY level, last_known_level)
)
SELECT
    id,
    adjuster_name,
    team,
    last_known_level_2,
    first_level,
    level_share_of_team,
    next2_level,
    level_vs_level_avg,
    level_percent_rank_2,
    rows_in_last_known_level_2_next2_level,
    level_last_known_level_sum_second_level,
    level_tercile_avg_2_prior,
    next2_level_sum_1_prior,
    last_known_level_share_of_team
FROM with_windows_2
ORDER BY last_known_level_2, next2_level
