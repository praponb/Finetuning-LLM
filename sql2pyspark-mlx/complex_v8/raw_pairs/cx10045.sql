WITH recent AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source,
        DATE_TRUNC('YEAR', played_at) AS played_at_year
    FROM plays
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        device,
        is_skipped,
        source,
        SIZE(COLLECT_SET(track_id) OVER (PARTITION BY listener_id, is_skipped)) AS distinct_track_id_in_listener_id_is_skipped,
        COUNT(*) OVER (PARTITION BY listener_id, is_skipped) AS rows_in_listener_id_is_skipped,
        FIRST_VALUE(played_at) OVER (PARTITION BY device, source ORDER BY played_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_played_at,
        ROUND(AVG(ms_played) OVER (PARTITION BY listener_id, is_skipped), 2) AS listener_id_is_skipped_avg_ms_played
    FROM recent
),
with_details AS (
    -- Enrich with dimension data
    SELECT s.*, t.moods AS track_moods
    FROM with_trends s
    JOIN tracks t ON t.id = s.track_id AND t.is_explicit = FALSE
    WHERE SIZE(t.moods) >= 2
),
summary AS (
    -- Summarise per group
    SELECT
        distinct_track_id_in_listener_id_is_skipped,
        track_id,
        DATE_TRUNC('MONTH', first_played_at) AS first_played_at_month,
        COUNT(*) FILTER (WHERE device IS NOT NULL) AS n_device_is_not_null,
        ROUND(100 * COUNT(CASE WHEN device = 'desktop' THEN 1 END) / COUNT(*), 1) AS pct_device_desktop
    FROM with_details
    WHERE device IS NOT NULL
    GROUP BY distinct_track_id_in_listener_id_is_skipped, track_id, DATE_TRUNC('MONTH', first_played_at)
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        *,
        MAX(n_device_is_not_null) OVER (PARTITION BY track_id, YEAR(first_played_at_month)) AS track_id_first_played_at_month_year_max_n_device,
        ROUND(pct_device_desktop - AVG(pct_device_desktop) OVER (PARTITION BY track_id, YEAR(first_played_at_month)), 2) AS pct_device_desktop_vs_track_id_first_played_at_m,
        SUM(n_device_is_not_null) OVER w_track_id_first_played_at_mon AS track_id_first_played_at_month_year_sum_n_device,
        COUNT(*) OVER (PARTITION BY track_id, YEAR(first_played_at_month)) AS rows_in_track_id_first_played_at_month_year,
        COUNT(*) OVER (PARTITION BY distinct_track_id_in_listener_id_is_skipped, track_id ORDER BY n_device_is_not_null, distinct_track_id_in_listener_id_is_skipped, track_id, first_played_at_month ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count
    FROM summary
    WINDOW w_track_id_first_played_at_mon AS (PARTITION BY track_id, YEAR(first_played_at_month))
),
tagged AS (
    -- Add calculated columns
    SELECT
        *,
        COALESCE(running_count, 0) AS running_count_filled,
        ROUND(pct_device_desktop * 0.9, 2) AS pct_device_desktop_adj,
        DATE_TRUNC('HOUR', first_played_at_month) AS first_played_at_month_hour
    FROM sequenced
)
-- Final projection
SELECT
    distinct_track_id_in_listener_id_is_skipped,
    first_played_at_month,
    n_device_is_not_null,
    pct_device_desktop,
    track_id_first_played_at_month_year_max_n_device,
    pct_device_desktop_vs_track_id_first_played_at_m,
    track_id_first_played_at_month_year_sum_n_device,
    rows_in_track_id_first_played_at_month_year,
    running_count,
    running_count_filled,
    pct_device_desktop_adj,
    first_played_at_month_hour,
    DAYOFWEEK(first_played_at_month) AS first_played_at_month_dayofweek,
    ROUND(pct_device_desktop_adj * 1.5, 2) AS pct_device_desktop_adj_adj
FROM tagged
WHERE rows_in_track_id_first_played_at_month_year >= 9
ORDER BY pct_device_desktop_adj
