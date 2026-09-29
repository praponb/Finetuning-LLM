WITH valid_tracks AS (
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
        ABS(bpm - 142) AS bpm_distance
    FROM tracks
    WHERE ARRAY_CONTAINS(moods, 'sad')
      AND LOWER(title) LIKE '%lo%'
)
SELECT duration_sec, title AS label
FROM valid_tracks
EXCEPT
SELECT id, country
FROM listeners
