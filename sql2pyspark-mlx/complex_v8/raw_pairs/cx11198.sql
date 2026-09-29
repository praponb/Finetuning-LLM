WITH filtered AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        device,
        is_skipped,
        source,
        LOWER(TRIM(device)) AS device_clean
    FROM plays
    WHERE track_id <= 28
)
SELECT
    id,
    listener_id,
    track_id,
    device,
    is_skipped,
    device_clean
FROM filtered
WHERE source IS NOT NULL
