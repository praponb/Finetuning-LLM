WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        genre,
        country,
        debut_year,
        ABS(debut_year - 1997) AS debut_year_distance
    FROM artists
    WHERE is_verified = TRUE
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        MAX(debut_year_distance) OVER (PARTITION BY genre, country) AS genre_country_max_debut_year_distance,
        ROUND(AVG(debut_year_distance) OVER (PARTITION BY genre, country), 2) AS genre_country_avg_debut_year_distance,
        MAX(debut_year) OVER w_genre_country AS genre_country_max_debut_year,
        SUM(debut_year_distance) OVER (PARTITION BY country ORDER BY debut_year NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_debut_year_distance
    FROM cleaned
    WINDOW w_genre_country AS (PARTITION BY genre, country)
),
genre_stats AS (
    -- Avg debut_year per genre
    SELECT genre, AVG(debut_year) AS avg_debut_year
    FROM with_trends
    GROUP BY genre
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_debut_year,
        ROUND(s.debut_year - g.avg_debut_year, 2) AS debut_year_minus_avg
    FROM with_trends s
    JOIN genre_stats g ON g.genre = s.genre
    WHERE s.debut_year > g.avg_debut_year
),
line_level_tracks AS (
    -- One row per track
    SELECT
        s.id,
        s.genre,
        s.country,
        s.debut_year_distance,
        s.genre_country_avg_debut_year_distance,
        s.genre_country_max_debut_year,
        s.running_debut_year_distance,
        s.debut_year_minus_avg,
        t.id AS track_id,
        t.moods AS track_moods,
        t.title AS track_title
    FROM vs_group s
    JOIN tracks t ON t.artist_id = s.id
)
-- Final projection
SELECT
    id,
    genre,
    country,
    genre_country_max_debut_year,
    running_debut_year_distance,
    debut_year_minus_avg,
    track_id,
    track_moods,
    LOWER(REGEXP_REPLACE(TRIM(genre), ' ', '_')) AS genre_slug,
    CONCAT(country, '#', CAST(id AS STRING)) AS country_ref
FROM line_level_tracks
