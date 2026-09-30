WITH cleaned AS (
    -- Filter artists
    SELECT
        id,
        artist_name,
        genre,
        country,
        debut_year,
        is_verified
    FROM artists
    WHERE debut_year BETWEEN 1991 AND 2018
      AND UPPER(TRIM(artist_name)) LIKE 'P%'
),
qualified AS (
    -- Rows with no matching tracks
    SELECT s.*
    FROM cleaned s
    WHERE NOT EXISTS (
            SELECT 1
            FROM tracks t
            WHERE t.artist_id = s.id
              AND ARRAY_CONTAINS(t.moods, 'chill')
        )
),
qualified_2 AS (
    -- Rows that have matching tracks
    SELECT s.*
    FROM qualified s
    WHERE EXISTS (
            SELECT 1
            FROM tracks t
            WHERE t.artist_id = s.id
              AND t.duration_sec < 353
        )
      AND s.is_verified = FALSE
),
filtered AS (
    -- Rows with no matching tracks
    SELECT s.*
    FROM qualified_2 s
    WHERE NOT EXISTS (
            SELECT 1
            FROM tracks t
            WHERE t.artist_id = s.id
              AND UPPER(TRIM(t.title)) LIKE 'R%'
        )
)
-- Final projection
SELECT
    id,
    artist_name,
    country,
    debut_year,
    is_verified
FROM filtered
