WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source
    FROM plays
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        SUM(ms_played) OVER w_played_at_month_source AS played_at_month_source_sum_ms_played,
        ROUND((UNIX_TIMESTAMP(played_at) - UNIX_TIMESTAMP(LAG(played_at) OVER w_all_by_played_at)) / 60, 1) AS minutes_since_prev_played_at,
        ROW_NUMBER() OVER (PARTITION BY is_skipped, device ORDER BY ms_played NULLS LAST, id) AS ms_played_seq,
        COUNT(*) OVER (PARTITION BY MONTH(played_at), source) AS rows_in_played_at_month_source,
        MAX(ms_played) OVER w_played_at_month_source AS played_at_month_source_max_ms_played,
        NTH_VALUE(ms_played, 2) OVER (PARTITION BY is_skipped, listener_id ORDER BY played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_ms_played,
        COUNT(*) OVER (ORDER BY played_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        DENSE_RANK() OVER w_all_by_played_at AS played_at_dense_rank,
        NTH_VALUE(played_at, 2) OVER (PARTITION BY is_skipped, device ORDER BY ms_played NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_played_at,
        FIRST_VALUE(played_at) OVER (PARTITION BY is_skipped, device ORDER BY ms_played NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_played_at,
        NTILE(5) OVER (PARTITION BY is_skipped, listener_id ORDER BY played_at, id) AS played_at_quintile,
        LAST_VALUE(played_at) OVER (PARTITION BY is_skipped, device ORDER BY ms_played NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_played_at,
        ROUND(PERCENT_RANK() OVER (PARTITION BY is_skipped, device ORDER BY ms_played NULLS LAST, id), 3) AS ms_played_percent_rank,
        DENSE_RANK() OVER (PARTITION BY is_skipped, listener_id ORDER BY played_at, id) AS played_at_dense_rank_2,
        LEAD(ms_played, 2) OVER w_is_skipped_device_by_ms_play AS next2_ms_played
    FROM scoped
    WINDOW w_played_at_month_source AS (PARTITION BY MONTH(played_at), source), w_all_by_played_at AS (ORDER BY played_at, id), w_is_skipped_device_by_ms_play AS (PARTITION BY is_skipped, device ORDER BY ms_played NULLS LAST, id)
),
top_per_group AS (
    -- First row per group
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source,
        played_at_month_source_sum_ms_played,
        minutes_since_prev_played_at,
        ms_played_seq,
        rows_in_played_at_month_source,
        played_at_month_source_max_ms_played,
        second_ms_played,
        running_count,
        played_at_dense_rank,
        second_played_at,
        first_played_at,
        played_at_quintile,
        last_played_at,
        ms_played_percent_rank,
        next2_ms_played
    FROM windowed
    WHERE played_at_dense_rank_2 = 1
),
windowed_2 AS (
    -- Previous / next values per key
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        is_skipped,
        source,
        played_at_month_source_sum_ms_played,
        minutes_since_prev_played_at,
        rows_in_played_at_month_source,
        second_ms_played,
        running_count,
        second_played_at,
        first_played_at,
        ms_played_percent_rank,
        next2_ms_played,
        RANK() OVER w_ms_played_seq_by_minutes_sin AS minutes_since_prev_played_at_rank,
        SIZE(COLLECT_SET(device) OVER (PARTITION BY source)) AS distinct_device_in_source,
        LAST_VALUE(ms_played) OVER (ORDER BY ms_played ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_ms_played,
        MIN(played_at_dense_rank) OVER (PARTITION BY ms_played_seq ORDER BY minutes_since_prev_played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_played_at_dense_rank_so_far,
        SUM(ms_played) OVER (ORDER BY played_at_month_source_max_ms_played DESC NULLS FIRST, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS ms_played_sum_7,
        ROUND(ms_played_percent_rank / NULLIF(SUM(ms_played_percent_rank) OVER (PARTITION BY source), 0), 4) AS ms_played_percent_rank_share_of_source,
        minutes_since_prev_played_at - LAG(minutes_since_prev_played_at) OVER (ORDER BY ms_played ASC, id) AS minutes_since_prev_played_at_change
    FROM top_per_group
    WINDOW w_ms_played_seq_by_minutes_sin AS (PARTITION BY ms_played_seq ORDER BY minutes_since_prev_played_at, id)
)
SELECT
    id,
    listener_id,
    track_id,
    ms_played,
    is_skipped,
    source,
    played_at_month_source_sum_ms_played,
    rows_in_played_at_month_source,
    second_ms_played,
    running_count,
    second_played_at,
    ms_played_percent_rank,
    minutes_since_prev_played_at_rank,
    distinct_device_in_source,
    last_ms_played,
    ms_played_sum_7,
    ms_played_percent_rank_share_of_source,
    minutes_since_prev_played_at_change,
    DATE_FORMAT(second_played_at, 'yyyy-MM') AS second_played_at_label
FROM windowed_2
WHERE ms_played BETWEEN 67932 AND 350343
ORDER BY ms_played_sum_7 DESC, track_id NULLS LAST, id
LIMIT 5
