WITH filtered AS (
    -- Filter adjusters
    SELECT
        id,
        adjuster_name,
        team,
        level,
        COALESCE(level, 0) AS level_filled,
        SUBSTRING(TRIM(adjuster_name), 1, 3) AS adjuster_name_prefix
    FROM adjusters
    WHERE level <= 3
      AND UPPER(TRIM(adjuster_name)) LIKE 'E%'
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        id,
        adjuster_name,
        team,
        level_filled,
        adjuster_name_prefix,
        LAST_VALUE(level_filled) OVER (PARTITION BY level_filled, level ORDER BY level ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_level_filled,
        COUNT(*) OVER (PARTITION BY level_filled, level ORDER BY level ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        SUM(level) OVER (PARTITION BY level ORDER BY level_filled DESC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS level_sum_2,
        level_filled - LAG(level_filled) OVER (PARTITION BY level_filled, level ORDER BY level ASC, id) AS level_filled_change,
        level - LAG(level) OVER (PARTITION BY level_filled, level ORDER BY level ASC, id) AS level_change,
        NTH_VALUE(level, 2) OVER (ORDER BY level DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_level,
        LAG(level_filled) OVER (PARTITION BY team, level ORDER BY level_filled NULLS LAST, id) AS prev_level_filled,
        MIN(level_filled) OVER w_level_level_filled AS level_level_filled_min_level_filled,
        ROUND(level_filled / NULLIF(SUM(level_filled) OVER w_level_level_filled, 0), 4) AS level_filled_share_of_level_level_filled,
        level - LAG(level) OVER w_team_level_by_level_filled AS level_change_2,
        LAG(level_filled, 3) OVER (PARTITION BY level ORDER BY level DESC, id) AS prev3_level_filled,
        ROW_NUMBER() OVER (PARTITION BY level ORDER BY level_filled DESC, id) AS seq_no,
        NTILE(4) OVER (ORDER BY level DESC NULLS FIRST, id) AS level_quartile,
        COUNT(*) OVER (PARTITION BY level, level_filled) AS rows_in_level_level_filled
    FROM filtered
    WINDOW w_level_level_filled AS (PARTITION BY level, level_filled), w_team_level_by_level_filled AS (PARTITION BY team, level ORDER BY level_filled NULLS LAST, id)
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        id,
        adjuster_name,
        level_filled,
        last_level_filled,
        level_sum_2,
        level_change,
        prev_level_filled,
        level_level_filled_min_level_filled,
        level_change_2,
        prev3_level_filled,
        seq_no,
        level_quartile,
        MAX(level_sum_2) OVER w_level_filled_seq_no AS level_filled_seq_no_max_level_sum_2,
        ROUND(seq_no / NULLIF(SUM(seq_no) OVER (PARTITION BY level_filled, seq_no), 0), 4) AS seq_no_share_of_level_filled_seq_no,
        ROUND(running_count / NULLIF(SUM(running_count) OVER w_level_filled_seq_no, 0), 4) AS running_count_share_of_level_filled_seq_no,
        MAX(level_quartile) OVER (ORDER BY level_level_filled_min_level_filled DESC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS level_quartile_max_3,
        SUM(prev_level_filled) OVER (PARTITION BY level_filled, seq_no) AS level_filled_seq_no_sum_prev_level_filled,
        FIRST_VALUE(second_level) IGNORE NULLS OVER (ORDER BY rows_in_level_level_filled, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_second_level,
        ROUND(level_quartile / NULLIF(SUM(level_quartile) OVER (PARTITION BY level_filled, seq_no), 0), 4) AS level_quartile_share_of_level_filled_seq_no,
        LEAD(level_change) OVER (ORDER BY rows_in_level_level_filled, id) AS next_level_change
    FROM windowed
    WINDOW w_level_filled_seq_no AS (PARTITION BY level_filled, seq_no)
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        level_change_2,
        ARRAY_SORT(COLLECT_SET(adjuster_name)) AS adjuster_name_list,
        SUM(CASE WHEN UPPER(TRIM(adjuster_name)) LIKE 'E%' THEN 1 ELSE 0 END) AS n_upper_trim_adjuster_name_lik,
        SUM(CASE WHEN UPPER(TRIM(adjuster_name)) LIKE 'F%' THEN 1 ELSE 0 END) AS n_upper_trim_adjuster_name_lik_2
    FROM with_trends
    GROUP BY 1
)
-- Final projection
SELECT
    level_change_2,
    adjuster_name_list,
    n_upper_trim_adjuster_name_lik,
    n_upper_trim_adjuster_name_lik_2,
    FLOOR(level_change_2 / 2) * 2 AS level_change_2_bucket,
    CASE WHEN level_change_2 >= 0 THEN 'large' WHEN level_change_2 >= -1 THEN 'medium' ELSE 'small' END AS level_change_2_tier
FROM summary
WHERE SIZE(adjuster_name_list) >= 2
ORDER BY level_change_2_bucket NULLS LAST, level_change_2
LIMIT 5
