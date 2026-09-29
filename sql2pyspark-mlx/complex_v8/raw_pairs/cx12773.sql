WITH base_plays AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        device,
        is_skipped,
        source,
        CONCAT(device, '#', CAST(ms_played AS STRING)) AS device_ref
    FROM plays
)
-- Final projection
SELECT
    id,
    listener_id,
    track_id,
    device,
    source,
    device_ref
FROM base_plays
WHERE device_ref IS NOT NULL
ORDER BY device_ref DESC, id DESC
