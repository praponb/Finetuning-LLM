WITH base AS (
    -- Filter artists
    SELECT
        id,
        artist_name,
        genre,
        country,
        debut_year
    FROM artists
    WHERE genre = 'Jazz'
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        *,
        RANK() OVER (PARTITION BY country, genre ORDER BY debut_year DESC, id) AS debut_year_rank,
        SUM(debut_year) OVER (PARTITION BY country, genre ORDER BY debut_year, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS debut_year_sum_7,
        RANK() OVER w_all_by_debut_year_ties AS debut_year_rank_2,
        ROUND(CUME_DIST() OVER (PARTITION BY country, genre ORDER BY debut_year DESC), 3) AS debut_year_cume_dist,
        ROUND(debut_year - AVG(debut_year) OVER (PARTITION BY country), 2) AS debut_year_vs_country_avg,
        DENSE_RANK() OVER (PARTITION BY country, genre ORDER BY debut_year ASC) AS debut_year_dense_rank,
        ROUND(CUME_DIST() OVER (PARTITION BY country, genre ORDER BY debut_year ASC), 3) AS debut_year_cume_dist_2,
        ROUND(AVG(debut_year) OVER w_country, 2) AS country_avg_debut_year,
        COUNT(*) OVER w_country AS rows_in_country,
        ROUND(debut_year - AVG(debut_year) OVER w_country_genre, 2) AS debut_year_vs_country_genre_avg,
        LEAD(debut_year, 3) OVER (PARTITION BY country, genre ORDER BY debut_year ASC, id) AS next3_debut_year
    FROM base
    WINDOW w_all_by_debut_year_ties AS (ORDER BY debut_year DESC), w_country AS (PARTITION BY country), w_country_genre AS (PARTITION BY country, genre)
)
SELECT
    id,
    genre,
    country,
    debut_year,
    debut_year_sum_7,
    debut_year_rank_2,
    debut_year_cume_dist,
    debut_year_dense_rank,
    country_avg_debut_year,
    rows_in_country,
    next3_debut_year
FROM ranked
ORDER BY country_avg_debut_year, debut_year_cume_dist DESC
