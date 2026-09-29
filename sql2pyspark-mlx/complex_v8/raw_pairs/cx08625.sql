WITH recent AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        device,
        source
    FROM plays
    WHERE TO_DATE(played_at) < DATE '2025-02-24'
),
joined_track AS (
    -- Join tracks
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.device,
        t.artist_id AS track_artist_id,
        t.moods AS track_moods
    FROM recent s
    JOIN tracks t ON t.id = s.track_id
)
-- Final projection
SELECT
    id,
    listener_id,
    track_id,
    device,
    track_artist_id,
    LENGTH(TRIM(device)) AS device_len
FROM joined_track
WHERE listener_id >= 2
ORDER BY device, id
