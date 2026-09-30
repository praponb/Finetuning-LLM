WITH valid AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        bpm,
        moods
    FROM tracks
    WHERE artist_id >= 5
),
without_matches AS (
    -- Keep rows whose artist matches
    SELECT s.*
    FROM valid s
    WHERE NOT EXISTS (
            SELECT 1
            FROM artists a
            WHERE a.id = s.artist_id
              AND a.genre NOT IN ('Jazz', 'Indie')
              AND a.country = 'TH'
        )
)
SELECT
    id,
    artist_id,
    duration_sec,
    release_date,
    bpm
FROM without_matches
ORDER BY duration_sec, id
LIMIT 10
