WITH base AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        device,
        is_skipped
    FROM plays
),
tagged AS (
    -- Label and score rows
    SELECT
        id,
        device,
        is_skipped,
        COALESCE(device, 'UNKNOWN') AS device_or_unknown,
        ABS(ms_played - 151112) AS ms_played_distance,
        CONCAT(device, '#', CAST(ms_played AS STRING)) AS device_ref
    FROM base
)
SELECT *
FROM (
    SELECT is_skipped, device, device_or_unknown, ms_played_distance
    FROM tagged
) src
PIVOT (
    MAX(ms_played_distance)
    FOR device_or_unknown IN ('car', 'UNKNOWN', 'desktop')
)
