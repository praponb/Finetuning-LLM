WITH base_artists AS (
    -- Scope the artists rows
    SELECT *
    FROM artists
    WHERE artist_name IS NOT NULL
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        artist_name,
        genre,
        country,
        debut_year,
        is_verified,
        LAST_VALUE(debut_year) IGNORE NULLS OVER (PARTITION BY country ORDER BY debut_year DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_debut_year,
        RANK() OVER (PARTITION BY country ORDER BY debut_year DESC NULLS FIRST, id) AS debut_year_rank,
        SUM(debut_year) OVER (PARTITION BY genre ORDER BY debut_year DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS debut_year_sum_2_prior,
        FIRST_VALUE(debut_year) IGNORE NULLS OVER (PARTITION BY country ORDER BY debut_year DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_debut_year,
        ROUND(AVG(debut_year) OVER (ORDER BY debut_year ASC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS debut_year_avg_7,
        SUM(debut_year) OVER (PARTITION BY genre, is_verified) AS genre_is_verified_sum_debut_year,
        ROUND(AVG(debut_year) OVER (PARTITION BY country), 2) AS country_avg_debut_year,
        NTH_VALUE(debut_year, 2) OVER (ORDER BY debut_year ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_debut_year,
        NTILE(5) OVER (PARTITION BY genre ORDER BY debut_year DESC, id) AS debut_year_quintile,
        COUNT(*) OVER w_country AS rows_in_country,
        ROUND(CUME_DIST() OVER (PARTITION BY country ORDER BY debut_year DESC NULLS FIRST), 3) AS debut_year_cume_dist
    FROM base_artists
    WINDOW w_country AS (PARTITION BY country)
)
-- Final projection
SELECT
    id,
    artist_name,
    debut_year,
    is_verified,
    last_known_debut_year,
    debut_year_rank,
    debut_year_sum_2_prior,
    first_known_debut_year,
    debut_year_avg_7,
    country_avg_debut_year,
    debut_year_cume_dist,
    CASE WHEN debut_year_cume_dist >= 0.6 THEN 'A' WHEN debut_year_cume_dist >= 0.3 THEN 'B' ELSE 'C' END AS debut_year_cume_dist_tier,
    LENGTH(TRIM(genre)) AS genre_len
FROM with_windows
ORDER BY debut_year, id
LIMIT 10
