WITH recent_plays AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        source,
        LENGTH(TRIM(device)) AS device_len,
        LENGTH(TRIM(device)) AS device_len_2
    FROM plays
    WHERE source <> 'playlist'
      AND played_at >= TIMESTAMP '2025-02-06 15:00:00'
),
enriched_track AS (
    -- Join tracks
    SELECT s.*, t.is_explicit AS track_is_explicit
    FROM recent_plays s
    JOIN tracks t ON t.id = s.track_id AND t.is_explicit = FALSE
    WHERE t.is_explicit = TRUE
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, metric, value
FROM (
    SELECT id, device_len, ms_played
    FROM enriched_track
) src
UNPIVOT (
    value FOR metric IN (device_len, ms_played)
) unp
ORDER BY id, metric
