WITH filtered_artists AS (
    -- Scope the artists rows
    SELECT
        id,
        artist_name,
        genre,
        country,
        debut_year,
        is_verified,
        COALESCE(country, 'UNKNOWN') AS country_or_unknown
    FROM artists
    WHERE genre NOT IN ('Pop', 'Jazz')
      AND NOT is_verified
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        LAG(debut_year, 1, 0) OVER w_country_or_unknown_genre_by AS prev_debut_year,
        FIRST_VALUE(debut_year) IGNORE NULLS OVER (PARTITION BY is_verified ORDER BY debut_year, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_debut_year,
        NTILE(10) OVER w_is_verified_by_debut_year AS debut_year_decile,
        debut_year - LAG(debut_year) OVER w_country_or_unknown_genre_by AS debut_year_change,
        debut_year - LAG(debut_year) OVER w_is_verified_by_debut_year AS debut_year_change_2,
        COUNT(*) OVER (PARTITION BY genre) AS rows_in_genre,
        ROUND(debut_year / NULLIF(SUM(debut_year) OVER (PARTITION BY genre), 0), 4) AS debut_year_share_of_genre,
        ROUND(PERCENT_RANK() OVER (ORDER BY debut_year ASC), 3) AS debut_year_percent_rank,
        MIN(debut_year) OVER w_genre AS genre_min_debut_year,
        SUM(debut_year) OVER (PARTITION BY country_or_unknown, genre ORDER BY debut_year NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_debut_year
    FROM filtered_artists
    WINDOW w_country_or_unknown_genre_by AS (PARTITION BY country_or_unknown, genre ORDER BY debut_year NULLS LAST, id), w_is_verified_by_debut_year AS (PARTITION BY is_verified ORDER BY debut_year, id), w_genre AS (PARTITION BY genre)
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        LEAD(debut_year_change, 2) OVER (PARTITION BY is_verified, country ORDER BY genre_min_debut_year, id) AS next2_debut_year_change,
        ROUND(AVG(rows_in_genre) OVER (PARTITION BY is_verified, country ORDER BY genre_min_debut_year, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS rows_in_genre_avg_3,
        SUM(running_debut_year) OVER w_debut_year_decile_country_or AS debut_year_decile_country_or_unknown_sum_running,
        NTILE(3) OVER (PARTITION BY is_verified, country ORDER BY genre_min_debut_year, id) AS genre_min_debut_year_tercile,
        ROUND(PERCENT_RANK() OVER (ORDER BY rows_in_genre ASC), 3) AS rows_in_genre_percent_rank,
        LAG(running_debut_year) OVER (PARTITION BY is_verified, country ORDER BY genre_min_debut_year, id) AS prev_running_debut_year,
        COUNT(*) OVER (PARTITION BY debut_year_decile, country_or_unknown) AS rows_in_debut_year_decile_country_or_unknown,
        DENSE_RANK() OVER (ORDER BY rows_in_genre ASC) AS rows_in_genre_dense_rank
    FROM metrics
    WINDOW w_debut_year_decile_country_or AS (PARTITION BY debut_year_decile, country_or_unknown)
)
-- Final projection
SELECT
    id,
    genre,
    country,
    debut_year,
    is_verified,
    country_or_unknown,
    prev_debut_year,
    first_known_debut_year,
    debut_year_decile,
    debut_year_change,
    debut_year_change_2,
    rows_in_genre,
    debut_year_share_of_genre,
    genre_min_debut_year,
    running_debut_year,
    next2_debut_year_change,
    debut_year_decile_country_or_unknown_sum_running,
    rows_in_genre_percent_rank,
    prev_running_debut_year,
    rows_in_genre_dense_rank
FROM with_trends
WHERE rows_in_genre_avg_3 >= 3.9
ORDER BY next2_debut_year_change DESC
