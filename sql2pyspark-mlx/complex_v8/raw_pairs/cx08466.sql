WITH valid_tracks AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        title,
        is_explicit,
        moods
    FROM tracks
    WHERE artist_id IS NOT NULL
)
SELECT id AS qty, title AS label
FROM valid_tracks
INTERSECT ALL
SELECT birth_year, country
FROM listeners
