WITH scoped_plays AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        device,
        is_skipped,
        TO_DATE(played_at) AS played_at_date,
        LENGTH(TRIM(device)) AS device_len
    FROM plays
),
with_details_track AS (
    -- Attach track attributes
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.device,
        s.is_skipped,
        s.played_at_date,
        s.device_len,
        t.is_explicit AS track_is_explicit,
        t.title AS track_title
    FROM scoped_plays s
    JOIN tracks t ON t.id = s.track_id
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        id,
        listener_id,
        track_id,
        device,
        track_is_explicit,
        track_title,
        ROUND(device_len - AVG(device_len) OVER (PARTITION BY track_is_explicit, track_id), 2) AS device_len_vs_track_is_explicit_track_id_avg,
        SIZE(COLLECT_SET(track_id) OVER w_is_skipped_listener_id) AS distinct_track_id_in_is_skipped_listener_id,
        ROUND(device_len - AVG(device_len) OVER (PARTITION BY is_skipped, listener_id), 2) AS device_len_vs_is_skipped_listener_id_avg,
        RANK() OVER (PARTITION BY track_is_explicit ORDER BY played_at_date ASC NULLS LAST, id) AS played_at_date_rank,
        NTH_VALUE(device_len, 2) OVER (ORDER BY played_at_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_device_len,
        MIN(device_len) OVER (PARTITION BY track_is_explicit, track_id) AS track_is_explicit_track_id_min_device_len,
        DATEDIFF(played_at_date, LAG(played_at_date) OVER (PARTITION BY track_is_explicit ORDER BY played_at_date ASC NULLS LAST, id)) AS days_since_prev_played_at_date,
        SIZE(COLLECT_SET(device) OVER w_track_is_explicit_track_id) AS distinct_device_in_track_is_explicit_track_id,
        RANK() OVER w_device_by_played_at_date_tie AS played_at_date_rank_2
    FROM with_details_track
    WINDOW w_is_skipped_listener_id AS (PARTITION BY is_skipped, listener_id), w_track_is_explicit_track_id AS (PARTITION BY track_is_explicit, track_id), w_device_by_played_at_date_tie AS (PARTITION BY device ORDER BY played_at_date DESC)
),
with_windows_2 AS (
    -- Previous / next values per key
    SELECT
        *,
        SIZE(COLLECT_SET(device) OVER (PARTITION BY played_at_date_rank_2, distinct_track_id_in_is_skipped_listener_id)) AS distinct_device_in_played_at_date_rank_2_distinc,
        LEAD(played_at_date_rank, 1, 0) OVER (PARTITION BY distinct_device_in_track_is_explicit_track_id ORDER BY device_len_vs_is_skipped_listener_id_avg, id) AS next_played_at_date_rank,
        ROUND(PERCENT_RANK() OVER (PARTITION BY played_at_date_rank_2, second_device_len ORDER BY played_at_date_rank), 3) AS played_at_date_rank_percent_rank,
        COUNT(*) OVER (PARTITION BY played_at_date_rank_2, distinct_track_id_in_is_skipped_listener_id) AS rows_in_played_at_date_rank_2_distinct_track_id_,
        ROUND(PERCENT_RANK() OVER (ORDER BY distinct_track_id_in_is_skipped_listener_id ASC), 3) AS distinct_track_id_in_is_skipped_listener_id_perc
    FROM with_windows
),
totals AS (
    -- Group-level totals
    SELECT
        second_device_len,
        listener_id,
        played_at_date_rank,
        COUNT(*) AS cnt,
        SUM(CASE WHEN NOT track_is_explicit THEN 1 ELSE 0 END) AS n_not_track_is_explicit,
        ARRAY_SORT(COLLECT_SET(track_title)) AS track_title_list,
        ROUND(STDDEV(days_since_prev_played_at_date), 2) AS stddev_days_since_prev_played_at_date
    FROM with_windows_2
    GROUP BY second_device_len, listener_id, played_at_date_rank
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        cnt - LAG(cnt) OVER (PARTITION BY listener_id ORDER BY n_not_track_is_explicit DESC, second_device_len, listener_id, played_at_date_rank) AS cnt_change,
        MIN(played_at_date_rank) OVER (PARTITION BY listener_id) AS listener_id_min_played_at_date_rank,
        NTH_VALUE(second_device_len, 2) OVER (PARTITION BY listener_id ORDER BY n_not_track_is_explicit DESC, second_device_len, listener_id, played_at_date_rank ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_second_device_len,
        ROUND(n_not_track_is_explicit - AVG(n_not_track_is_explicit) OVER w_listener_id, 2) AS n_not_track_is_explicit_vs_listener_id_avg
    FROM totals
    WINDOW w_listener_id AS (PARTITION BY listener_id)
)
-- Final projection
SELECT
    second_device_len,
    listener_id,
    n_not_track_is_explicit,
    track_title_list,
    stddev_days_since_prev_played_at_date,
    listener_id_min_played_at_date_rank,
    n_not_track_is_explicit_vs_listener_id_avg
FROM with_trends
ORDER BY second_device_len DESC
