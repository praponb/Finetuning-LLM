WITH base_artists AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_name,
        genre,
        country,
        debut_year,
        is_verified,
        COALESCE(debut_year, 0) AS debut_year_filled,
        CONCAT_WS(': ', genre, country) AS genre_country_label,
        CASE WHEN is_verified THEN 1 ELSE 0 END AS is_verified_flag
    FROM artists
    WHERE is_verified = FALSE
      AND country IS NOT NULL
),
filtered AS (
    -- Compare against the overall avg
    SELECT *
    FROM base_artists
    WHERE is_verified_flag > (SELECT AVG(is_verified_flag) FROM base_artists)
)
SELECT
    is_verified_flag,
    COALESCE(genre, 'ALL') AS genre,
    is_verified,
    SUM(CASE WHEN artist_name IS NOT NULL THEN debut_year ELSE 0 END) AS debut_year_artist_name_is_not_null,
    SUM(CASE WHEN genre = 'Indie' THEN 1 ELSE 0 END) AS n_genre_indie,
    ROUND(AVG(debut_year), 2) AS avg_debut_year
FROM filtered
GROUP BY GROUPING SETS ((is_verified_flag, genre, is_verified), (is_verified_flag), ())
ORDER BY is_verified_flag NULLS LAST, genre NULLS LAST, is_verified NULLS LAST
