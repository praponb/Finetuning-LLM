WITH filtered_tracks AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        moods
    FROM tracks
    WHERE duration_sec BETWEEN 142 AND 352
),
combined_details AS (
    -- Join artists
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.duration_sec,
        a.genre AS artist_genre,
        a.artist_name,
        a.is_verified AS artist_is_verified
    FROM filtered_tracks s
    JOIN artists a ON a.id = s.artist_id
    WHERE a.is_verified = FALSE
),
stats AS (
    -- Group-level totals
    SELECT
        artist_is_verified,
        artist_id,
        artist_genre,
        COUNT(*) AS row_count,
        COUNT(DISTINCT artist_genre) AS distinct_artist_genres,
        COUNT(DISTINCT id) AS distinct_ids
    FROM combined_details
    WHERE duration_sec <= 352
    GROUP BY 1, 2, 3
    HAVING COUNT(*) >= 2
),
screened AS (
    -- Rows that have matching tracks
    SELECT s.*
    FROM stats s
    WHERE EXISTS (
            SELECT 1
            FROM tracks t
            WHERE t.artist_id = s.artist_id
              AND t.bpm > 82
        )
),
with_details AS (
    -- Enrich with dimension data
    SELECT
        s.artist_is_verified,
        s.artist_id,
        s.artist_genre,
        s.row_count,
        s.distinct_artist_genres,
        s.distinct_ids,
        a.is_verified AS artist_is_verified_2,
        a.debut_year AS artist_debut_year,
        a.artist_name
    FROM screened s
    LEFT JOIN artists a ON a.id = s.artist_id
)
SELECT
    artist_is_verified,
    artist_id,
    artist_genre,
    row_count,
    distinct_artist_genres,
    distinct_ids,
    artist_name,
    row_count - artist_debut_year AS row_count_minus_artist_debut_year
FROM with_details
ORDER BY distinct_artist_genres
