WITH cleaned_plays AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        device,
        source,
        ABS(ms_played - 129336) AS ms_played_distance
    FROM plays
    WHERE listener_id IS NOT NULL
)
SELECT id AS ref_id
FROM cleaned_plays
WHERE track_id IS NOT NULL
  AND played_at >= TIMESTAMP '2025-02-06 15:00:00'
INTERSECT ALL
SELECT id
FROM artists
WHERE country <> 'US'
