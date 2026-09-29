WITH filtered_plays AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        is_skipped,
        source,
        DATE_TRUNC('HOUR', played_at) AS played_at_hour,
        CASE WHEN is_skipped THEN 1 ELSE 0 END AS is_skipped_flag
    FROM plays
    WHERE listener_id >= 3
      AND TO_DATE(played_at) < DATE '2025-02-18'
),
stats AS (
    -- Summarise per group
    SELECT
        is_skipped,
        MAX(ms_played) AS max_ms_played,
        ARRAY_SORT(COLLECT_SET(source)) AS source_list,
        MIN(is_skipped_flag) AS min_is_skipped_flag,
        SUM(CASE WHEN is_skipped = TRUE THEN 1 ELSE 0 END) AS n_is_skipped_true,
        MAX(is_skipped_flag) AS max_is_skipped_flag
    FROM filtered_plays
    GROUP BY 1
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER (ORDER BY max_ms_played ASC NULLS LAST, is_skipped), 3) AS max_ms_played_percent_rank,
        ROUND(max_is_skipped_flag - AVG(max_is_skipped_flag) OVER (PARTITION BY max_is_skipped_flag, min_is_skipped_flag), 2) AS max_is_skipped_flag_vs_max_is_skipped_flag_min_i
    FROM stats
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        is_skipped,
        max_ms_played,
        source_list,
        min_is_skipped_flag,
        n_is_skipped_true,
        max_ms_played_percent_rank,
        LEAD(max_ms_played, 3, 0) OVER (ORDER BY max_is_skipped_flag ASC, is_skipped) AS next3_max_ms_played,
        NTILE(5) OVER (ORDER BY max_is_skipped_flag ASC, is_skipped) AS max_is_skipped_flag_quintile,
        LAG(n_is_skipped_true, 1, 0) OVER w_max_is_skipped_flag_is_skipp AS prev_n_is_skipped_true
    FROM metrics
    WINDOW w_max_is_skipped_flag_is_skipp AS (PARTITION BY max_is_skipped_flag, is_skipped ORDER BY min_is_skipped_flag, is_skipped)
)
-- Native SQL UNPIVOT (wide to long)
SELECT is_skipped, metric, amount
FROM (
    SELECT is_skipped, n_is_skipped_true, prev_n_is_skipped_true
    FROM with_windows
) src
UNPIVOT (
    amount FOR metric IN (n_is_skipped_true AS n_is_skipped_true_v, prev_n_is_skipped_true AS m_prev_n_is_skipped_true)
) unp
