WITH valid_artists AS (
    -- Filter artists
    SELECT
        id,
        artist_name,
        genre,
        country,
        debut_year,
        is_verified
    FROM artists
    WHERE artist_name IS NOT NULL
      AND country <> 'BR'
),
enriched AS (
    -- Add calculated columns
    SELECT *, INITCAP(TRIM(artist_name)) AS artist_name_clean
    FROM valid_artists
    WHERE debut_year > 1996
),
summary AS (
    -- Summarise per group
    SELECT
        is_verified,
        country,
        genre,
        SUM(debut_year) AS total_debut_year
    FROM enriched
    GROUP BY is_verified, country, genre
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        is_verified,
        country,
        genre,
        SUM(total_debut_year) OVER (PARTITION BY genre) AS genre_sum_total_debut_year,
        COUNT(*) OVER (PARTITION BY genre) AS rows_in_genre,
        MAX(total_debut_year) OVER (PARTITION BY is_verified ORDER BY total_debut_year, is_verified, country, genre ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS total_debut_year_max_4,
        ROUND(total_debut_year - AVG(total_debut_year) OVER w_genre, 2) AS total_debut_year_vs_genre_avg
    FROM summary
    WINDOW w_genre AS (PARTITION BY genre)
)
-- Final projection
SELECT
    is_verified,
    country,
    genre,
    genre_sum_total_debut_year,
    rows_in_genre,
    total_debut_year_max_4,
    total_debut_year_vs_genre_avg,
    rows_in_genre - total_debut_year_max_4 AS rows_in_genre_minus_total_debut_year_max_4
FROM metrics
WHERE rows_in_genre <= 13
ORDER BY country
