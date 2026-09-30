WITH recent_plays AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        device,
        is_skipped,
        DATEDIFF(DATE '2025-06-30', played_at) AS days_since_played_at
    FROM plays
    WHERE NOT is_skipped
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        ROW_NUMBER() OVER w_all_by_days_since_played_at AS seq_no,
        ROUND(days_since_played_at - AVG(days_since_played_at) OVER w_listener_id_device, 2) AS days_since_played_at_vs_listener_id_device_avg,
        SIZE(COLLECT_SET(track_id) OVER (PARTITION BY listener_id, device)) AS distinct_track_id_in_listener_id_device,
        SUM(ms_played) OVER w_listener_id_device AS listener_id_device_sum_ms_played,
        LEAD(ms_played) OVER w_all_by_days_since_played_at AS next_ms_played
    FROM recent_plays
    WINDOW w_all_by_days_since_played_at AS (ORDER BY days_since_played_at, id), w_listener_id_device AS (PARTITION BY listener_id, device)
),
with_stats AS (
    -- Attach plays metrics
    SELECT
        s.*,
        (SELECT SUM(p.ms_played) FROM plays p WHERE p.track_id = s.track_id) AS total_play_ms_played,
        (SELECT SUM(p.ms_played) FROM plays p WHERE p.track_id = s.track_id) AS total_play_ms_played_2,
        (SELECT COUNT(CASE WHEN p.is_skipped = FALSE THEN 1 END) FROM plays p WHERE p.track_id = s.track_id) AS play_is_skipped_count
    FROM ranked s
)
SELECT
    COALESCE(device, 'ALL') AS device,
    listener_id,
    ROUND(AVG(days_since_played_at), 2) AS avg_days_since_played_at
FROM with_stats
GROUP BY CUBE(device, listener_id)
ORDER BY device NULLS LAST, listener_id NULLS LAST
