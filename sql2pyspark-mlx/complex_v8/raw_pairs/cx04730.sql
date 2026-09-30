WITH scoped_plays AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        is_skipped,
        source,
        CONCAT(device, '#', CAST(ms_played AS STRING)) AS device_ref,
        FLOOR(ms_played / 50000) * 50000 AS ms_played_bucket,
        DATE_FORMAT(played_at, 'yyyy-MM-dd HH:00') AS played_at_label
    FROM plays
    WHERE track_id IS NOT NULL
      AND played_at < TIMESTAMP '2025-03-08 08:00:00'
),
with_details_track AS (
    -- Attach track attributes
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.ms_played,
        s.is_skipped,
        s.ms_played_bucket,
        s.played_at_label,
        t.title AS track_title,
        t.artist_id AS track_artist_id,
        t.is_explicit AS track_is_explicit
    FROM scoped_plays s
    LEFT JOIN tracks t ON t.id = s.track_id AND t.duration_sec <= 402
)
SELECT
    track_is_explicit,
    is_skipped,
    MAX(played_at) AS last_played_at
FROM with_details_track
GROUP BY CUBE(track_is_explicit, is_skipped)
