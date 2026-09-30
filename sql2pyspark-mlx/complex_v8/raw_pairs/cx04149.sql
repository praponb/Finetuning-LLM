WITH valid_plays AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        device,
        is_skipped,
        source,
        HOUR(played_at) AS played_at_hour,
        IF(is_skipped, 'yes', 'no') AS is_skipped_label,
        FLOOR(ms_played / 50000) * 50000 AS ms_played_bucket
    FROM plays
    WHERE track_id <= 24
      AND source NOT IN ('radio', 'search')
),
detail_rows AS (
    -- One row per play
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.is_skipped,
        s.ms_played_bucket,
        p.id AS play_id,
        p.listener_id AS play_listener_id,
        p.device AS play_device
    FROM valid_plays s
    JOIN plays p ON p.track_id = s.track_id AND p.device <> 'phone'
)
SELECT
    id,
    listener_id,
    track_id,
    ms_played_bucket,
    play_id,
    IF(is_skipped, 'yes', 'no') AS is_skipped_label,
    FLOOR(ms_played_bucket / 50000) * 50000 AS ms_played_bucket_bucket
FROM detail_rows
ORDER BY id, play_id
LIMIT 5
