WITH prepared AS (
    -- Scope the artists rows
    SELECT
        id,
        artist_name,
        genre,
        country,
        debut_year,
        is_verified,
        COALESCE(debut_year, 0) AS debut_year_filled
    FROM artists
    WHERE country = 'BR'
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        artist_name,
        country,
        debut_year,
        NTH_VALUE(debut_year, 2) OVER (PARTITION BY country, is_verified ORDER BY debut_year_filled DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_debut_year,
        DENSE_RANK() OVER w_country_is_verified_by_debut_2 AS debut_year_filled_dense_rank,
        LAST_VALUE(debut_year_filled) OVER (PARTITION BY is_verified ORDER BY debut_year, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_debut_year_filled,
        LAST_VALUE(debut_year_filled) OVER (PARTITION BY genre ORDER BY debut_year ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_debut_year_filled_2,
        FIRST_VALUE(debut_year) OVER (PARTITION BY genre ORDER BY debut_year ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_debut_year,
        MIN(debut_year) OVER (PARTITION BY is_verified ORDER BY debut_year, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_debut_year_so_far,
        LAG(debut_year) OVER (PARTITION BY genre, is_verified ORDER BY debut_year DESC, id) AS prev_debut_year,
        SIZE(COLLECT_SET(genre) OVER w_is_verified) AS distinct_genre_in_is_verified
    FROM prepared
    WINDOW w_country_is_verified_by_debut_2 AS (PARTITION BY country, is_verified ORDER BY debut_year_filled DESC), w_is_verified AS (PARTITION BY is_verified)
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        distinct_genre_in_is_verified,
        debut_year_filled_dense_rank,
        PERCENTILE_APPROX(prev_debut_year, 0.5) AS median_prev_debut_year,
        SUM(debut_year_filled_dense_rank) FILTER (WHERE country = 'US') AS debut_year_filled_dense_rank_country_us,
        SUM(CASE WHEN country = 'KR' THEN 1 ELSE 0 END) AS n_country_kr
    FROM metrics
    GROUP BY distinct_genre_in_is_verified, debut_year_filled_dense_rank
    HAVING AVG(last_debut_year_filled) > 2003
)
-- Final projection
SELECT
    distinct_genre_in_is_verified,
    debut_year_filled_dense_rank,
    debut_year_filled_dense_rank_country_us,
    LEAST(n_country_kr, median_prev_debut_year) AS least_n_country_kr_median_prev_debut_year
FROM summary
ORDER BY distinct_genre_in_is_verified DESC NULLS FIRST, debut_year_filled_dense_rank DESC
