WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        device,
        source,
        FLOOR(ms_played / 50000) * 50000 AS ms_played_bucket
    FROM plays
    WHERE track_id IS NOT NULL
)
SELECT
    id,
    listener_id,
    track_id,
    ms_played,
    device,
    ms_played_bucket
FROM filtered
WHERE ms_played > 73335
ORDER BY listener_id NULLS LAST, ms_played_bucket DESC
