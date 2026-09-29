WITH valid_plays AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        device,
        is_skipped,
        CONCAT_WS(': ', source, device) AS source_device_label,
        played_at + INTERVAL 1 DAYS AS played_at_plus_1d,
        ABS(ms_played - 129336) AS ms_played_distance
    FROM plays
    WHERE track_id IS NOT NULL
      AND is_skipped = TRUE
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        SIZE(COLLECT_SET(track_id) OVER w_is_skipped_listener_id) AS distinct_track_id_in_is_skipped_listener_id,
        SIZE(COLLECT_SET(device) OVER (PARTITION BY is_skipped, listener_id)) AS distinct_device_in_is_skipped_listener_id,
        COUNT(*) OVER (PARTITION BY is_skipped, listener_id) AS rows_in_is_skipped_listener_id
    FROM valid_plays
    WINDOW w_is_skipped_listener_id AS (PARTITION BY is_skipped, listener_id)
),
joined AS (
    -- Join tracks
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.ms_played,
        s.is_skipped,
        s.played_at_plus_1d,
        s.ms_played_distance,
        s.distinct_track_id_in_is_skipped_listener_id,
        s.distinct_device_in_is_skipped_listener_id,
        s.rows_in_is_skipped_listener_id,
        t.release_date AS track_release_date,
        t.bpm AS track_bpm,
        t.is_explicit AS track_is_explicit
    FROM metrics s
    LEFT JOIN tracks t ON t.id = s.track_id
)
SELECT
    id,
    listener_id,
    track_id,
    played_at_plus_1d,
    ms_played_distance,
    distinct_track_id_in_is_skipped_listener_id,
    distinct_device_in_is_skipped_listener_id,
    track_release_date,
    track_bpm,
    track_is_explicit,
    CASE WHEN ms_played_distance >= 300000 THEN 'large' WHEN ms_played_distance >= 80000 THEN 'medium' ELSE 'small' END AS ms_played_distance_tier
FROM joined
ORDER BY track_bpm, id DESC
