WITH cleaned AS (
    -- Filter tracks
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
    WHERE artist_id >= 8
      AND UPPER(TRIM(title)) LIKE 'G%'
)
SELECT bpm AS ref_id, title
FROM cleaned
UNION
SELECT id AS ref_id, genre AS title
FROM artists
