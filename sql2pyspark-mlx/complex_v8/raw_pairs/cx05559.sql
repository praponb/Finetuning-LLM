WITH recent_plays AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        SUBSTRING(TRIM(device), 1, 2) AS device_prefix
    FROM plays
)
SELECT id AS qty
FROM recent_plays
WHERE ms_played BETWEEN 107912 AND 308104
EXCEPT
SELECT id
FROM plays
WHERE device = 'tv'
ORDER BY qty
