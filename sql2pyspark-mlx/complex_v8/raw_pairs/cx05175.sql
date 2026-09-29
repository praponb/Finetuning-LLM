WITH filtered AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        device,
        source
    FROM plays
),
derived AS (
    -- Label and score rows
    SELECT *, COALESCE(device, 'UNKNOWN') AS device_or_unknown
    FROM filtered
    WHERE track_id IS NOT NULL
)
SELECT
    id,
    listener_id,
    track_id,
    ms_played,
    device,
    source,
    device_or_unknown
FROM derived
ORDER BY listener_id
