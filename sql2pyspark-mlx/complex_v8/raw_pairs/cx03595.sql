WITH scoped_plays AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        is_skipped,
        source,
        FLOOR(ms_played / 50000) * 50000 AS ms_played_bucket
    FROM plays
)
SELECT
    id,
    track_id,
    is_skipped,
    source,
    ms_played_bucket,
    ROUND(ms_played / NULLIF(ms_played_bucket, 0), 3) AS ms_played_per_ms_played_bucket,
    ROUND(ms_played_bucket / NULLIF(ms_played, 0), 3) AS ms_played_bucket_per_ms_played
FROM scoped_plays
WHERE source <> 'playlist'
ORDER BY track_id, ms_played_per_ms_played_bucket DESC
