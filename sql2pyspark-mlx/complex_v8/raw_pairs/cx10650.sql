WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_name,
        genre,
        country,
        debut_year,
        is_verified,
        CASE WHEN debut_year >= 2018 THEN 'HIGH' WHEN debut_year >= 1996 THEN 'MEDIUM' ELSE 'LOW' END AS debut_year_tier
    FROM artists
    WHERE LENGTH(TRIM(artist_name)) >= 7
      AND genre IN ('HipHop', 'Indie')
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        DENSE_RANK() OVER (ORDER BY debut_year DESC, id) AS debut_year_dense_rank,
        NTILE(5) OVER (PARTITION BY is_verified, debut_year_tier ORDER BY debut_year, id) AS debut_year_quintile,
        COUNT(*) OVER (PARTITION BY is_verified, country) AS rows_in_is_verified_country,
        LAST_VALUE(debut_year) IGNORE NULLS OVER (PARTITION BY genre ORDER BY debut_year DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_debut_year,
        SIZE(COLLECT_SET(genre) OVER w_is_verified_country) AS distinct_genre_in_is_verified_country,
        RANK() OVER (ORDER BY debut_year ASC) AS debut_year_rank,
        LEAD(debut_year) OVER w_genre_by_debut_year AS next_debut_year,
        MAX(debut_year) OVER (PARTITION BY is_verified, country) AS is_verified_country_max_debut_year,
        LAG(debut_year, 2) OVER (PARTITION BY genre ORDER BY debut_year DESC NULLS FIRST, id) AS prev2_debut_year,
        LAG(debut_year) OVER (ORDER BY debut_year ASC, id) AS prev_debut_year,
        ROUND(debut_year / NULLIF(SUM(debut_year) OVER (PARTITION BY is_verified, country), 0), 4) AS debut_year_share_of_is_verified_country,
        ROUND(AVG(debut_year) OVER (PARTITION BY is_verified, debut_year_tier ORDER BY debut_year, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS debut_year_avg_7,
        SUM(debut_year) OVER w_is_verified_country AS is_verified_country_sum_debut_year,
        debut_year - LAG(debut_year) OVER w_all_by_debut_year AS debut_year_change
    FROM recent
    WINDOW w_is_verified_country AS (PARTITION BY is_verified, country), w_genre_by_debut_year AS (PARTITION BY genre ORDER BY debut_year DESC NULLS FIRST, id), w_all_by_debut_year AS (ORDER BY debut_year DESC, id)
),
enriched AS (
    -- Add calculated columns
    SELECT
        id,
        artist_name,
        country,
        is_verified,
        debut_year_dense_rank,
        debut_year_quintile,
        last_known_debut_year,
        distinct_genre_in_is_verified_country,
        debut_year_rank,
        next_debut_year,
        is_verified_country_max_debut_year,
        prev_debut_year,
        debut_year_avg_7,
        is_verified_country_sum_debut_year,
        debut_year_change,
        ROUND(debut_year_rank / NULLIF(debut_year_change, 0), 3) AS debut_year_rank_per_debut_year_change,
        debut_year_dense_rank - last_known_debut_year AS debut_year_dense_rank_minus_last_known_debut_yea
    FROM ranked
    WHERE debut_year_rank > 4
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        id,
        debut_year_quintile,
        debut_year_rank,
        next_debut_year,
        prev_debut_year,
        debut_year_avg_7,
        debut_year_change,
        debut_year_dense_rank_minus_last_known_debut_yea,
        ROUND(CUME_DIST() OVER (PARTITION BY debut_year_quintile, distinct_genre_in_is_verified_country ORDER BY is_verified_country_max_debut_year NULLS LAST, id), 3) AS is_verified_country_max_debut_year_cume_dist,
        FIRST_VALUE(debut_year_change) OVER (PARTITION BY distinct_genre_in_is_verified_country, is_verified ORDER BY debut_year_dense_rank_minus_last_known_debut_yea, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_debut_year_change,
        NTH_VALUE(debut_year_avg_7, 2) OVER (PARTITION BY debut_year_quintile, distinct_genre_in_is_verified_country ORDER BY is_verified_country_max_debut_year NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_debut_year_avg_7,
        debut_year_avg_7 - LAG(debut_year_avg_7) OVER w_debut_year_quintile_distinct AS debut_year_avg_7_change,
        ROUND(AVG(debut_year_change) OVER (PARTITION BY is_verified), 2) AS is_verified_avg_debut_year_change
    FROM enriched
    WINDOW w_debut_year_quintile_distinct AS (PARTITION BY debut_year_quintile, distinct_genre_in_is_verified_country ORDER BY is_verified_country_max_debut_year NULLS LAST, id)
)
SELECT
    id,
    debut_year_rank,
    next_debut_year,
    prev_debut_year,
    debut_year_avg_7,
    debut_year_change,
    debut_year_dense_rank_minus_last_known_debut_yea,
    is_verified_country_max_debut_year_cume_dist,
    first_debut_year_change,
    second_debut_year_avg_7,
    is_verified_avg_debut_year_change
FROM sequenced
ORDER BY next_debut_year NULLS LAST, second_debut_year_avg_7 NULLS LAST
