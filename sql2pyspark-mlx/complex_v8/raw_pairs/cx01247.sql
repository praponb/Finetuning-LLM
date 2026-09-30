WITH recent_tracks AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        title,
        release_date,
        bpm,
        moods
    FROM tracks
    WHERE duration_sec <= 402
),
screened AS (
    -- Rows that have matching tracks
    SELECT s.*
    FROM recent_tracks s
    WHERE EXISTS (
            SELECT 1
            FROM tracks t
            WHERE t.artist_id = s.artist_id
        )
),
with_details_artist AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.release_date,
        s.bpm,
        s.moods,
        a.debut_year AS artist_debut_year,
        a.genre AS artist_genre,
        a.is_verified AS artist_is_verified
    FROM screened s
    JOIN artists a ON a.id = s.artist_id AND a.is_verified = FALSE
)
-- Subtotals with ROLLUP
SELECT
    artist_genre,
    artist_id,
    GROUPING_ID() AS grouping_level,
    MIN(artist_debut_year) AS min_artist_debut_year
FROM with_details_artist
GROUP BY ROLLUP(artist_genre, artist_id)
ORDER BY artist_genre NULLS LAST, artist_id NULLS LAST
