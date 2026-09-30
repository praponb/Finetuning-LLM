WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_name,
        genre,
        country,
        debut_year,
        is_verified,
        IF(is_verified, 'yes', 'no') AS is_verified_label
    FROM artists
    WHERE genre = 'Pop'
      AND UPPER(TRIM(artist_name)) LIKE 'K%'
),
genre_stats AS (
    -- Max debut_year per genre
    SELECT genre, MAX(debut_year) AS max_debut_year
    FROM cleaned
    GROUP BY genre
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_debut_year,
        ROUND(s.debut_year - g.max_debut_year, 2) AS debut_year_minus_max
    FROM cleaned s
    JOIN genre_stats g ON g.genre = s.genre
    WHERE s.debut_year = g.max_debut_year
)
SELECT
    is_verified,
    genre,
    GROUPING(is_verified) AS is_total_is_verified,
    ROUND(AVG(debut_year_minus_max), 2) AS avg_debut_year_minus_max
FROM vs_group
GROUP BY CUBE(is_verified, genre)
ORDER BY is_verified NULLS LAST, genre NULLS LAST
