WITH valid AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        device,
        is_skipped,
        LOWER(TRIM(device)) AS device_clean
    FROM plays
    WHERE source = 'radio'
)
SELECT
    id,
    listener_id,
    track_id,
    ms_played,
    device,
    is_skipped,
    FLOOR(ms_played / 50000) * 50000 AS ms_played_bucket
FROM valid
ORDER BY track_id DESC, device DESC NULLS FIRST, id
LIMIT 10
