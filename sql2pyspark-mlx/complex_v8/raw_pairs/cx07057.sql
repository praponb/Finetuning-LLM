WITH prepared AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods
    FROM tracks
    WHERE is_explicit = TRUE
)
SELECT id AS ref_id, title
FROM prepared
UNION
SELECT id AS ref_id, artist_name AS title
FROM artists
WHERE debut_year < 2016
ORDER BY title, ref_id
