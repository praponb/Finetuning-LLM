WITH valid_tracks AS (
    -- Filter tracks
    SELECT *
    FROM tracks
    WHERE artist_id IS NOT NULL
)
SELECT title AS category
FROM valid_tracks
WHERE is_explicit = TRUE
INTERSECT
SELECT device AS category
FROM plays
WHERE played_at < TIMESTAMP '2025-02-24 12:00:00'
