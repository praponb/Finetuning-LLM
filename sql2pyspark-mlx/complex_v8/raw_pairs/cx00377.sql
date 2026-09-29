WITH prepared_artists AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_name,
        genre,
        country,
        debut_year,
        is_verified,
        ABS(debut_year - 1996) AS debut_year_distance,
        IF(is_verified, 'yes', 'no') AS is_verified_label
    FROM artists
    WHERE country IS NOT NULL
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        *,
        LEAD(debut_year) OVER (ORDER BY debut_year ASC, id) AS next_debut_year,
        FIRST_VALUE(debut_year) OVER (PARTITION BY is_verified ORDER BY debut_year DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_debut_year,
        ROUND(CUME_DIST() OVER (PARTITION BY is_verified ORDER BY debut_year_distance), 3) AS debut_year_distance_cume_dist,
        COUNT(*) OVER (PARTITION BY genre, is_verified) AS rows_in_genre_is_verified,
        ROUND(debut_year - AVG(debut_year) OVER (PARTITION BY genre, is_verified), 2) AS debut_year_vs_genre_is_verified_avg,
        LAST_VALUE(debut_year_distance) IGNORE NULLS OVER (PARTITION BY is_verified ORDER BY debut_year_distance, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_debut_year_distance,
        SIZE(COLLECT_SET(genre) OVER (PARTITION BY is_verified_label)) AS distinct_genre_in_is_verified_label,
        LAST_VALUE(debut_year) IGNORE NULLS OVER (ORDER BY debut_year ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_debut_year,
        SUM(debut_year_distance) OVER (PARTITION BY is_verified ORDER BY debut_year DESC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS debut_year_distance_sum_1_prior,
        NTILE(5) OVER w_is_verified_is_verified_labe AS debut_year_distance_quintile
    FROM prepared_artists
    WINDOW w_is_verified_is_verified_labe AS (PARTITION BY is_verified, is_verified_label ORDER BY debut_year_distance DESC NULLS FIRST, id)
),
derived AS (
    -- Add calculated columns
    SELECT
        id,
        genre,
        country,
        debut_year,
        is_verified_label,
        next_debut_year,
        first_debut_year,
        debut_year_vs_genre_is_verified_avg,
        debut_year_distance_sum_1_prior,
        debut_year_distance_quintile,
        last_known_debut_year - next_debut_year AS last_known_debut_year_minus_next_debut_year
    FROM sequenced
    WHERE next_debut_year BETWEEN 1994 AND 2015
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        debut_year_distance_quintile,
        genre,
        ROUND(SUM(debut_year_vs_genre_is_verified_avg), 2) AS total_debut_year_vs_genre_is_verified_avg,
        ROUND(STDDEV(debut_year_distance_sum_1_prior), 2) AS stddev_debut_year_distance_sum_1_prior,
        ARRAY_SORT(COLLECT_SET(country)) AS country_list,
        ROUND(SUM(last_known_debut_year_minus_next_debut_year) / NULLIF(SUM(debut_year_distance_sum_1_prior), 0), 3) AS last_known_debut_year_minus_next_debut_year_to_d,
        SUM(CASE WHEN genre <> 'HipHop' THEN 1 ELSE 0 END) AS n_genre_hiphop
    FROM derived
    WHERE debut_year_vs_genre_is_verified_avg <= 606.9
    GROUP BY debut_year_distance_quintile, genre
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        debut_year_distance_quintile,
        genre,
        country_list,
        last_known_debut_year_minus_next_debut_year_to_d,
        ROUND(debut_year_distance_quintile / NULLIF(SUM(debut_year_distance_quintile) OVER (PARTITION BY debut_year_distance_quintile), 0), 4) AS debut_year_distance_quintile_share_of_debut_year,
        SUM(n_genre_hiphop) OVER (PARTITION BY n_genre_hiphop ORDER BY n_genre_hiphop, debut_year_distance_quintile, genre ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_n_genre_hiphop
    FROM stats
)
SELECT
    debut_year_distance_quintile,
    country_list,
    last_known_debut_year_minus_next_debut_year_to_d,
    debut_year_distance_quintile_share_of_debut_year,
    running_n_genre_hiphop,
    CASE WHEN running_n_genre_hiphop >= 70 THEN 'large' WHEN running_n_genre_hiphop >= 40 THEN 'medium' ELSE 'small' END AS running_n_genre_hiphop_tier
FROM with_windows
ORDER BY running_n_genre_hiphop DESC, running_n_genre_hiphop_tier
