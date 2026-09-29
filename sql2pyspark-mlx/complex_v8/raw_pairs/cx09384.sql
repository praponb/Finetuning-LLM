WITH recent_adjusters AS (
    -- Filter adjusters
    SELECT
        id,
        adjuster_name,
        team,
        level
    FROM adjusters
),
windowed AS (
    -- Previous / next values per key
    SELECT
        id,
        team,
        level,
        ROUND(level - AVG(level) OVER (PARTITION BY level), 2) AS level_vs_level_avg,
        NTH_VALUE(level, 2) OVER (ORDER BY level, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_level,
        ROUND(PERCENT_RANK() OVER (ORDER BY level), 3) AS level_percent_rank,
        ROUND(level / NULLIF(SUM(level) OVER (PARTITION BY level), 0), 4) AS level_share_of_level,
        NTILE(4) OVER (ORDER BY level, id) AS level_quartile,
        level - LAG(level) OVER w_level_by_level AS level_change,
        LAG(level, 1, 0) OVER w_all_by_level_2 AS prev_level,
        COUNT(*) OVER (PARTITION BY level) AS rows_in_level,
        ROUND(AVG(level) OVER (PARTITION BY level), 2) AS level_avg_level
    FROM recent_adjusters
    WINDOW w_level_by_level AS (PARTITION BY level ORDER BY level NULLS LAST, id), w_level AS (PARTITION BY level), w_all_by_level_2 AS (ORDER BY level DESC, id)
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        MAX(level_percent_rank) OVER w_second_level_team AS second_level_team_max_level_percent_rank,
        RANK() OVER (ORDER BY level_vs_level_avg ASC) AS level_vs_level_avg_rank,
        DENSE_RANK() OVER (ORDER BY level_vs_level_avg ASC, id) AS level_vs_level_avg_dense_rank,
        ROUND(prev_level / NULLIF(SUM(prev_level) OVER w_second_level_team, 0), 4) AS prev_level_share_of_second_level_team,
        FIRST_VALUE(level_avg_level) IGNORE NULLS OVER (PARTITION BY team, prev_level ORDER BY second_level DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_level_avg_level,
        ROUND(rows_in_level - AVG(rows_in_level) OVER w_second_level_team, 2) AS rows_in_level_vs_second_level_team_avg,
        ROUND(second_level / NULLIF(SUM(second_level) OVER (PARTITION BY second_level, team), 0), 4) AS second_level_share_of_second_level_team,
        ROW_NUMBER() OVER w_team_prev_level_by_second_le_2 AS seq_no
    FROM windowed
    WINDOW w_second_level_team AS (PARTITION BY second_level, team), w_team_prev_level_by_second_le_2 AS (PARTITION BY team, prev_level ORDER BY second_level DESC, id)
),
totals AS (
    -- Summarise per group
    SELECT
        level_vs_level_avg_rank,
        SUM(CASE WHEN team = 'Motor' THEN 1 ELSE 0 END) AS n_team_motor,
        COUNT(DISTINCT id) AS distinct_ids,
        COUNT(*) AS cnt,
        SUM(first_known_level_avg_level) FILTER (WHERE team IS NOT NULL) AS first_known_level_avg_level_team_is_not_null
    FROM with_trends
    GROUP BY level_vs_level_avg_rank
)
SELECT
    level_vs_level_avg_rank,
    n_team_motor,
    distinct_ids,
    cnt,
    first_known_level_avg_level_team_is_not_null,
    FLOOR(cnt / 2) * 2 AS cnt_bucket,
    FLOOR(n_team_motor / 2) * 2 AS n_team_motor_bucket
FROM totals
WHERE level_vs_level_avg_rank < 9
ORDER BY cnt NULLS LAST
