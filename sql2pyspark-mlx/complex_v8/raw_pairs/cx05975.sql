WITH recent_plays AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        device,
        is_skipped,
        source,
        FLOOR(ms_played / 50000) * 50000 AS ms_played_bucket
    FROM plays
    WHERE ms_played > 176302
      AND device IS NOT NULL
)
SELECT track_id AS qty
FROM recent_plays
INTERSECT
SELECT id AS qty
FROM tracks
ORDER BY qty
