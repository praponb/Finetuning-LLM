WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        bpm,
        LOWER(TRIM(title)) AS title_clean,
        LENGTH(TRIM(title)) AS title_len,
        DATEDIFF(DATE '2026-01-01', release_date) AS days_since_release_date
    FROM tracks
    WHERE duration_sec > 154
),
enriched_artist AS (
    -- Join artists
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.duration_sec,
        s.release_date,
        s.title_clean,
        s.title_len,
        a.artist_name,
        a.debut_year AS artist_debut_year,
        a.genre AS artist_genre
    FROM scoped s
    JOIN artists a ON a.id = s.artist_id
    WHERE LENGTH(TRIM(a.artist_name)) >= 5
),
with_parent AS (
    -- Attach artist attributes
    SELECT
        s.*,
        a.debut_year AS artist_debut_year_2,
        a.artist_name AS artist_artist_name,
        a.genre AS artist_genre_2
    FROM enriched_artist s
    JOIN artists a ON a.id = s.artist_id
)
-- Subtotals with GROUPING SETS
SELECT
    artist_genre,
    artist_genre_2,
    COUNT(*) FILTER (WHERE LOWER(artist_artist_name) LIKE '%er%') AS n_lower_artist_artist_name_lik,
    PERCENTILE_APPROX(title_len, 0.5) AS median_title_len,
    COUNT(*) AS row_count
FROM with_parent
GROUP BY GROUPING SETS ((artist_genre, artist_genre_2), (artist_genre), ())
