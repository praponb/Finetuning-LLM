WITH base_plays AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        device,
        is_skipped,
        source,
        COALESCE(ms_played, 0) AS ms_played_filled,
        COALESCE(device, 'UNKNOWN') AS device_or_unknown,
        CASE WHEN is_skipped THEN 1 ELSE 0 END AS is_skipped_flag
    FROM plays
    WHERE played_at >= TIMESTAMP '2025-01-13 12:00:00'
      AND track_id IS NOT NULL
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(AVG(ms_played_filled) OVER (PARTITION BY listener_id), 2) AS listener_id_avg_ms_played_filled,
        FIRST_VALUE(is_skipped_flag) OVER (PARTITION BY is_skipped ORDER BY is_skipped_flag, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_is_skipped_flag,
        NTH_VALUE(is_skipped_flag, 2) OVER (PARTITION BY is_skipped ORDER BY is_skipped_flag, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_is_skipped_flag,
        ROUND(ms_played_filled / NULLIF(SUM(ms_played_filled) OVER (PARTITION BY listener_id), 0), 4) AS ms_played_filled_share_of_listener_id,
        ROUND(PERCENT_RANK() OVER (PARTITION BY is_skipped ORDER BY is_skipped_flag, id), 3) AS is_skipped_flag_percent_rank
    FROM base_plays
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(first_is_skipped_flag / NULLIF(SUM(first_is_skipped_flag) OVER w_device, 0), 4) AS first_is_skipped_flag_share_of_device,
        MAX(is_skipped_flag_percent_rank) OVER (PARTITION BY device) AS device_max_is_skipped_flag_percent_rank,
        LEAD(is_skipped_flag_percent_rank, 1, 0) OVER (ORDER BY ms_played_filled_share_of_listener_id, id) AS next_is_skipped_flag_percent_rank,
        MIN(is_skipped_flag_percent_rank) OVER (PARTITION BY device) AS device_min_is_skipped_flag_percent_rank,
        SUM(first_is_skipped_flag) OVER (PARTITION BY device) AS device_sum_first_is_skipped_flag,
        RANK() OVER w_device_by_second_is_skipped AS second_is_skipped_flag_rank
    FROM metrics
    WINDOW w_device AS (PARTITION BY device), w_device_by_second_is_skipped AS (PARTITION BY device ORDER BY second_is_skipped_flag, id)
),
sequenced_2 AS (
    -- Window metrics over several partitions
    SELECT
        id,
        listener_id,
        ms_played,
        is_skipped,
        ms_played_filled,
        device_or_unknown,
        is_skipped_flag,
        first_is_skipped_flag,
        second_is_skipped_flag,
        device_max_is_skipped_flag_percent_rank,
        next_is_skipped_flag_percent_rank,
        device_sum_first_is_skipped_flag,
        second_is_skipped_flag_rank,
        RANK() OVER (PARTITION BY source ORDER BY ms_played) AS ms_played_rank,
        SUM(ms_played) OVER w_is_skipped_flag_second_is_sk AS is_skipped_flag_second_is_skipped_flag_rank_sum_,
        SIZE(COLLECT_SET(listener_id) OVER (PARTITION BY is_skipped_flag, second_is_skipped_flag_rank)) AS distinct_listener_id_in_is_skipped_flag_second_i,
        ROUND(ms_played - AVG(ms_played) OVER (PARTITION BY is_skipped_flag, second_is_skipped_flag_rank), 2) AS ms_played_vs_is_skipped_flag_second_is_skipped_f
    FROM sequenced
    WINDOW w_is_skipped_flag_second_is_sk AS (PARTITION BY is_skipped_flag, second_is_skipped_flag_rank)
),
tagged AS (
    -- Derive helper columns
    SELECT *, IF(is_skipped, 'yes', 'no') AS is_skipped_label
    FROM sequenced_2
    WHERE next_is_skipped_flag_percent_rank BETWEEN 0.1 AND 0.8
),
is_skipped_label_stats AS (
    -- Max first_is_skipped_flag per is_skipped_label
    SELECT is_skipped_label, MAX(first_is_skipped_flag) AS max_first_is_skipped_flag
    FROM tagged
    GROUP BY is_skipped_label
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_first_is_skipped_flag,
        ROUND(s.first_is_skipped_flag - g.max_first_is_skipped_flag, 2) AS first_is_skipped_flag_minus_max
    FROM tagged s
    JOIN is_skipped_label_stats g ON g.is_skipped_label = s.is_skipped_label
    WHERE s.first_is_skipped_flag = g.max_first_is_skipped_flag
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        MAX(ms_played) OVER (PARTITION BY second_is_skipped_flag ORDER BY is_skipped_flag_second_is_skipped_flag_rank_sum_ DESC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS ms_played_max_2,
        ROUND(max_first_is_skipped_flag / NULLIF(SUM(max_first_is_skipped_flag) OVER w_is_skipped_label, 0), 4) AS max_first_is_skipped_flag_share_of_is_skipped_la
    FROM outliers
    WINDOW w_is_skipped_label AS (PARTITION BY is_skipped_label)
)
SELECT
    id,
    listener_id,
    is_skipped,
    ms_played_filled,
    device_or_unknown,
    is_skipped_flag,
    second_is_skipped_flag,
    device_max_is_skipped_flag_percent_rank,
    next_is_skipped_flag_percent_rank,
    device_sum_first_is_skipped_flag,
    second_is_skipped_flag_rank,
    distinct_listener_id_in_is_skipped_flag_second_i,
    is_skipped_label,
    max_first_is_skipped_flag,
    first_is_skipped_flag_minus_max,
    ms_played_max_2,
    max_first_is_skipped_flag_share_of_is_skipped_la,
    ms_played + ms_played_max_2 AS ms_played_plus_ms_played_max_2
FROM windowed
ORDER BY id DESC, first_is_skipped_flag_minus_max DESC
LIMIT 10
