WITH base AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        duration_sec,
        is_explicit,
        bpm,
        moods,
        CONCAT(title, '#', CAST(id AS STRING)) AS title_ref
    FROM tracks
    WHERE LOWER(title) LIKE '%ig%'
),
without_matches AS (
    -- Rows with no matching tracks
    SELECT s.*
    FROM base s
    WHERE NOT EXISTS (
            SELECT 1
            FROM tracks t
            WHERE t.artist_id = s.artist_id
              AND ARRAY_CONTAINS(t.moods, 'workout')
              AND UPPER(TRIM(t.title)) LIKE 'S%'
        )
)
SELECT
    is_explicit,
    artist_id,
    SUM(duration_sec) AS total_duration_sec
FROM without_matches
GROUP BY CUBE(is_explicit, artist_id)
ORDER BY is_explicit NULLS LAST, artist_id NULLS LAST
