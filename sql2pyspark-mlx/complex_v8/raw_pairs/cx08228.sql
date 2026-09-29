WITH cleaned_tracks AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        duration_sec,
        release_date,
        bpm
    FROM tracks
    WHERE bpm < 142
)
SELECT 'track' AS origin, duration_sec, release_date
FROM cleaned_tracks
UNION ALL
SELECT 'listener' AS origin, birth_year, signup_date
FROM listeners
WHERE plan IS NOT NULL
  AND LENGTH(TRIM(username)) >= 4
