WITH filtered_plays AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source,
        CONCAT_WS('-', source, device) AS source_device_label
    FROM plays
    WHERE is_skipped = TRUE
),
enriched AS (
    -- Attach track attributes
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.ms_played,
        s.device,
        s.is_skipped,
        s.source,
        s.source_device_label,
        t.bpm AS track_bpm,
        t.release_date AS track_release_date
    FROM filtered_plays s
    JOIN tracks t ON t.id = s.track_id
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        MAX(ms_played) OVER w_listener_id AS listener_id_max_ms_played,
        RANK() OVER w_is_skipped_source_by_track_b AS track_bpm_rank,
        SUM(ms_played) OVER (PARTITION BY source ORDER BY track_release_date ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_ms_played,
        MIN(ms_played) OVER (PARTITION BY listener_id) AS listener_id_min_ms_played,
        COUNT(*) OVER (PARTITION BY listener_id) AS rows_in_listener_id,
        ROW_NUMBER() OVER (PARTITION BY is_skipped, source ORDER BY track_bpm, id) AS rn,
        NTH_VALUE(track_bpm, 2) OVER (PARTITION BY is_skipped, source ORDER BY track_bpm, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_track_bpm,
        MAX(ms_played) OVER (PARTITION BY source ORDER BY track_release_date ASC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS ms_played_max_7,
        FIRST_VALUE(played_at) IGNORE NULLS OVER (PARTITION BY track_id ORDER BY ms_played, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_played_at,
        NTH_VALUE(ms_played, 2) OVER (PARTITION BY is_skipped, source ORDER BY track_bpm, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_ms_played,
        SIZE(COLLECT_SET(device) OVER (PARTITION BY is_skipped, DATE_TRUNC('MONTH', played_at))) AS distinct_device_in_is_skipped_played_at_month
    FROM enriched
    WINDOW w_listener_id AS (PARTITION BY listener_id), w_is_skipped_source_by_track_b AS (PARTITION BY is_skipped, source ORDER BY track_bpm)
),
top_rows AS (
    -- First row per group
    SELECT *
    FROM with_windows
    WHERE track_bpm_rank = 1
),
with_windows_2 AS (
    -- Running and moving aggregates
    SELECT
        *,
        running_ms_played - LAG(running_ms_played) OVER (PARTITION BY source ORDER BY ms_played_max_7 NULLS LAST, id) AS running_ms_played_change,
        MAX(listener_id_min_ms_played) OVER (PARTITION BY distinct_device_in_is_skipped_played_at_month) AS distinct_device_in_is_skipped_played_at_month_ma,
        ROW_NUMBER() OVER (PARTITION BY source ORDER BY ms_played_max_7 NULLS LAST, id) AS rn_2,
        MIN(second_track_bpm) OVER (PARTITION BY distinct_device_in_is_skipped_played_at_month) AS distinct_device_in_is_skipped_played_at_month_mi
    FROM top_rows
),
totals AS (
    -- Group-level totals
    SELECT
        rn_2,
        DATE_TRUNC('MONTH', track_release_date) AS track_release_date_month,
        rn,
        MIN(track_release_date) AS first_track_release_date,
        COUNT(*) AS cnt,
        MAX(distinct_device_in_is_skipped_played_at_month) AS max_distinct_device_in_is_skipped_played_at_mont,
        COUNT(*) FILTER (WHERE device = 'tv') AS n_device_tv,
        MIN(listener_id_max_ms_played) AS min_listener_id_max_ms_played,
        ROUND(AVG(ms_played_max_7), 2) AS avg_ms_played_max_7
    FROM with_windows_2
    WHERE rows_in_listener_id <= 14
    GROUP BY rn_2, DATE_TRUNC('MONTH', track_release_date), rn
    HAVING COUNT(*) >= 3
)
-- Final projection
SELECT
    rn_2,
    rn,
    first_track_release_date,
    max_distinct_device_in_is_skipped_played_at_mont,
    n_device_tv,
    COALESCE(n_device_tv, 0) AS n_device_tv_filled,
    ROUND(avg_ms_played_max_7 * 0.25, 2) AS avg_ms_played_max_7_adj
FROM totals
WHERE max_distinct_device_in_is_skipped_played_at_mont < 7
ORDER BY n_device_tv_filled DESC NULLS FIRST
