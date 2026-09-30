WITH prepared AS (
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
        FLOOR(bpm / 25) * 25 AS bpm_bucket
    FROM tracks
    WHERE YEAR(release_date) = 2024
      AND LENGTH(TRIM(title)) >= 8
),
enriched_artist AS (
    -- Join artists
    SELECT s.*, a.is_verified AS artist_is_verified, a.artist_name
    FROM prepared s
    JOIN artists a ON a.id = s.artist_id
    WHERE a.artist_name IS NOT NULL
)
SELECT id, field, value
FROM (
    SELECT id, bpm_bucket, duration_sec, bpm
    FROM enriched_artist
) src
UNPIVOT (
    value FOR field IN (bpm_bucket, duration_sec, bpm)
) unp
ORDER BY id, field
