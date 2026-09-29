WITH base AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        duration_sec,
        release_date,
        is_explicit,
        DATEDIFF(DATE '2025-12-31', release_date) AS days_since_release_date
    FROM tracks
    WHERE ARRAY_CONTAINS(moods, 'chill')
      AND title IS NOT NULL
)
SELECT artist_id, title
FROM tracks
INTERSECT
SELECT bpm, title
FROM tracks
