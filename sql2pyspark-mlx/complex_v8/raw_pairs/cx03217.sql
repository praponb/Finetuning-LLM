WITH base AS (
    -- Scope the artists rows
    SELECT
        id,
        artist_name,
        genre,
        country,
        is_verified,
        CONCAT(genre, '#', CAST(debut_year AS STRING)) AS genre_ref,
        ABS(debut_year - 2006) AS debut_year_distance
    FROM artists
    WHERE is_verified = FALSE
      AND debut_year > 1997
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        debut_year_distance - LAG(debut_year_distance) OVER (PARTITION BY country ORDER BY debut_year_distance DESC NULLS FIRST, id) AS debut_year_distance_change,
        MIN(debut_year_distance) OVER w_country_genre AS country_genre_min_debut_year_distance,
        FIRST_VALUE(debut_year_distance) IGNORE NULLS OVER (PARTITION BY country ORDER BY debut_year_distance DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_debut_year_distance,
        NTILE(10) OVER (PARTITION BY country ORDER BY debut_year_distance DESC NULLS FIRST, id) AS debut_year_distance_decile
    FROM base
    WINDOW w_country_genre AS (PARTITION BY country, genre)
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        genre,
        country,
        SUM(debut_year_distance) AS total_debut_year_distance,
        ROUND(SUM(country_genre_min_debut_year_distance) / NULLIF(SUM(debut_year_distance_decile), 0), 3) AS country_genre_min_debut_year_distance_to_debut_y,
        ARRAY_SORT(COLLECT_SET(genre)) AS genre_list,
        COUNT(*) FILTER (WHERE UPPER(TRIM(artist_name)) LIKE 'L%') AS n_upper_trim_artist_name_like
    FROM windowed
    GROUP BY genre, country
    HAVING AVG(debut_year_distance_decile) > 3
),
without_matches AS (
    -- Compare against the overall avg
    SELECT *
    FROM stats
    WHERE country_genre_min_debut_year_distance_to_debut_y > (SELECT AVG(country_genre_min_debut_year_distance_to_debut_y) FROM stats)
),
qualified AS (
    -- Compare against the overall avg
    SELECT *
    FROM without_matches
    WHERE country_genre_min_debut_year_distance_to_debut_y > (SELECT AVG(country_genre_min_debut_year_distance_to_debut_y) FROM without_matches)
)
-- Subtotals with GROUPING SETS
SELECT
    n_upper_trim_artist_name_like,
    genre,
    ROUND(AVG(n_upper_trim_artist_name_like), 2) AS avg_n_upper_trim_artist_name_like
FROM qualified
GROUP BY GROUPING SETS ((n_upper_trim_artist_name_like, genre), (n_upper_trim_artist_name_like))
ORDER BY n_upper_trim_artist_name_like NULLS LAST, genre NULLS LAST
