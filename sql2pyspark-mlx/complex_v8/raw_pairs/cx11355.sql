WITH prepared_plays AS (
    -- Filter plays
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
    WHERE (played_at >= TIMESTAMP '2025-01-31 09:00:00' OR source = 'album')
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(played_at) OVER w_device_source_by_played_at AS next_played_at,
        MIN(ms_played) OVER (PARTITION BY source) AS source_min_ms_played,
        ROUND(ms_played - AVG(ms_played) OVER (PARTITION BY source), 2) AS ms_played_vs_source_avg,
        NTH_VALUE(ms_played, 2) OVER (PARTITION BY MONTH(played_at), is_skipped ORDER BY played_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_ms_played,
        FIRST_VALUE(ms_played) OVER (PARTITION BY listener_id ORDER BY played_at DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_ms_played,
        LEAD(ms_played) OVER (PARTITION BY device, source ORDER BY played_at, id) AS next_ms_played,
        LAG(ms_played, 1, 0) OVER (PARTITION BY listener_id ORDER BY played_at DESC NULLS FIRST, id) AS prev_ms_played,
        NTH_VALUE(played_at, 2) OVER (PARTITION BY device, source ORDER BY played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_played_at,
        ROUND(PERCENT_RANK() OVER (PARTITION BY listener_id ORDER BY played_at DESC NULLS FIRST, id), 3) AS played_at_percent_rank
    FROM prepared_plays
    WINDOW w_device_source_by_played_at AS (PARTITION BY device, source ORDER BY played_at, id)
),
derived AS (
    -- Add calculated columns
    SELECT
        id,
        track_id,
        played_at,
        ms_played,
        is_skipped,
        source_min_ms_played,
        ms_played_vs_source_avg,
        second_ms_played,
        next_ms_played,
        ROUND(ms_played_vs_source_avg * 1.07, 2) AS ms_played_vs_source_avg_adj,
        LENGTH(TRIM(device)) AS device_len,
        MONTH(next_played_at) AS next_played_at_month
    FROM with_trends
    WHERE ms_played_vs_source_avg >= -200000.0
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        track_id,
        played_at,
        ms_played_vs_source_avg,
        second_ms_played,
        ms_played_vs_source_avg_adj,
        next_played_at_month,
        ROUND(PERCENT_RANK() OVER (ORDER BY device_len, id), 3) AS device_len_percent_rank,
        ROUND(next_ms_played / NULLIF(SUM(next_ms_played) OVER (PARTITION BY device_len), 0), 4) AS next_ms_played_share_of_device_len,
        NTH_VALUE(device_len, 2) OVER (PARTITION BY next_played_at_month, YEAR(played_at) ORDER BY next_played_at_month DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_device_len,
        SIZE(COLLECT_SET(track_id) OVER (PARTITION BY device_len)) AS distinct_track_id_in_device_len,
        ROUND(CUME_DIST() OVER w_next_played_at_month_played_2, 3) AS next_played_at_month_cume_dist,
        SUM(source_min_ms_played) OVER (ORDER BY device_len, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_source_min_ms_played,
        COUNT(*) OVER (PARTITION BY device_len) AS rows_in_device_len,
        LAST_VALUE(ms_played_vs_source_avg) IGNORE NULLS OVER (ORDER BY device_len, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_ms_played_vs_source_avg
    FROM derived
    WINDOW w_next_played_at_month_played_2 AS (PARTITION BY next_played_at_month, YEAR(played_at) ORDER BY next_played_at_month DESC, id)
),
per_group AS (
    -- Group-level totals
    SELECT
        next_played_at_month,
        track_id,
        ROUND(STDDEV(ms_played_vs_source_avg_adj), 2) AS stddev_ms_played_vs_source_avg_adj,
        COUNT(DISTINCT id) AS distinct_ids,
        ROUND(STDDEV(device_len_percent_rank), 2) AS stddev_device_len_percent_rank,
        COUNT(*) AS row_count
    FROM ranked
    GROUP BY next_played_at_month, track_id
    HAVING COUNT(*) >= 2
),
with_trends_2 AS (
    -- Previous / next values per key
    SELECT
        next_played_at_month,
        track_id,
        distinct_ids,
        stddev_device_len_percent_rank,
        ROUND(AVG(distinct_ids) OVER (PARTITION BY track_id, next_played_at_month), 2) AS track_id_next_played_at_month_avg_distinct_ids,
        MAX(next_played_at_month) OVER w_track_id_next_played_at_mont AS track_id_next_played_at_month_max_next_played_at
    FROM per_group
    WINDOW w_track_id_next_played_at_mont AS (PARTITION BY track_id, next_played_at_month)
)
-- Final projection
SELECT
    next_played_at_month,
    track_id,
    distinct_ids,
    stddev_device_len_percent_rank,
    track_id_next_played_at_month_avg_distinct_ids,
    track_id_next_played_at_month_max_next_played_at,
    next_played_at_month - distinct_ids AS next_played_at_month_minus_distinct_ids
FROM with_trends_2
ORDER BY next_played_at_month_minus_distinct_ids DESC, track_id DESC, next_played_at_month
LIMIT 10
