WITH valid AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        LOWER(TRIM(title)) AS title_clean,
        DATE_TRUNC('MONTH', release_date) AS release_date_month
    FROM tracks
    WHERE bpm BETWEEN 85 AND 180
      AND release_date >= DATE '2024-09-26'
),
with_parent AS (
    -- Join artists
    SELECT
        s.id,
        s.artist_id,
        s.duration_sec,
        s.release_date,
        s.is_explicit,
        s.bpm,
        s.title_clean,
        s.release_date_month,
        a.genre AS artist_genre,
        a.debut_year AS artist_debut_year,
        a.is_verified AS artist_is_verified
    FROM valid s
    JOIN artists a ON a.id = s.artist_id
),
with_details_artist AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.artist_id,
        s.duration_sec,
        s.release_date,
        s.is_explicit,
        s.bpm,
        s.title_clean,
        s.release_date_month,
        s.artist_genre,
        s.artist_debut_year,
        s.artist_is_verified,
        a.genre AS artist_genre_2,
        a.debut_year AS artist_debut_year_2
    FROM with_parent s
    JOIN artists a ON a.id = s.artist_id
    WHERE a.debut_year <= 2020
)
-- Subtotals with ROLLUP
SELECT
    artist_genre_2,
    artist_is_verified,
    artist_genre,
    GROUPING(artist_genre_2) AS is_total_artist_genre_2,
    MAX(release_date) AS last_release_date,
    SUM(CASE WHEN is_explicit = TRUE THEN 1 ELSE 0 END) AS n_is_explicit_true
FROM with_details_artist
GROUP BY ROLLUP(artist_genre_2, artist_is_verified, artist_genre)
ORDER BY artist_genre_2 NULLS LAST, artist_is_verified NULLS LAST, artist_genre NULLS LAST
