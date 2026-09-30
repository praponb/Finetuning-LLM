WITH base AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device
    FROM plays
    WHERE NOT is_skipped
      AND source IS NOT NULL
)
SELECT
    track_id,
    device AS label,
    played_at AS happened_at,
    'play' AS record_type
FROM base
UNION ALL
SELECT
    listener_id,
    source,
    played_at,
    'play_2' AS record_type
FROM plays
ORDER BY happened_at, track_id
