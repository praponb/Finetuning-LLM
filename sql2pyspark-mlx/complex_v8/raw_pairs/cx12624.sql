WITH scoped_plays AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        device,
        source
    FROM plays
)
SELECT
    id,
    listener_id,
    track_id,
    ms_played
FROM scoped_plays
ORDER BY id DESC NULLS FIRST
