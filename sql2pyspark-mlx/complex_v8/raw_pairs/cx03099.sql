WITH cleaned_tracks AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods,
        CONCAT(title, '#', CAST(artist_id AS STRING)) AS title_ref
    FROM tracks
    WHERE duration_sec < 352
)
SELECT id AS entity_id, release_date
FROM cleaned_tracks
WHERE title IS NOT NULL
UNION ALL
SELECT birth_year, signup_date
FROM listeners
UNION ALL
SELECT bpm, release_date
FROM tracks
ORDER BY entity_id DESC
