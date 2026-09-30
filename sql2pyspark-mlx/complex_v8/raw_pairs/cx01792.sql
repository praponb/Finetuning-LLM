WITH cleaned_artists AS (
    -- Scope the artists rows
    SELECT
        id,
        artist_name,
        country,
        debut_year,
        is_verified,
        UPPER(TRIM(artist_name)) AS artist_name_clean,
        ABS(debut_year - 1997) AS debut_year_distance
    FROM artists
    WHERE (debut_year <= 2016 OR is_verified = FALSE)
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        id,
        artist_name,
        is_verified,
        debut_year_distance,
        NTH_VALUE(debut_year, 2) OVER (PARTITION BY is_verified ORDER BY debut_year DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_debut_year,
        SUM(debut_year_distance) OVER w_country_is_verified AS country_is_verified_sum_debut_year_distance,
        debut_year_distance - LAG(debut_year_distance) OVER (PARTITION BY country, is_verified ORDER BY debut_year_distance DESC, id) AS debut_year_distance_change,
        ROUND(AVG(debut_year) OVER (PARTITION BY is_verified, country ORDER BY debut_year ASC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS debut_year_avg_6_prior,
        ROW_NUMBER() OVER (PARTITION BY country, is_verified ORDER BY debut_year ASC, id) AS seq_no,
        ROUND(CUME_DIST() OVER w_all_by_debut_year_ties, 3) AS debut_year_cume_dist,
        NTH_VALUE(debut_year_distance, 2) OVER (ORDER BY debut_year DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_debut_year_distance,
        ROUND(CUME_DIST() OVER (PARTITION BY country, is_verified ORDER BY debut_year_distance DESC, id), 3) AS debut_year_distance_cume_dist,
        SUM(debut_year_distance) OVER (PARTITION BY country, is_verified ORDER BY debut_year_distance DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_debut_year_distance,
        NTH_VALUE(debut_year_distance, 2) OVER (PARTITION BY is_verified ORDER BY debut_year DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_debut_year_distance_2,
        debut_year - LAG(debut_year) OVER (PARTITION BY country ORDER BY debut_year DESC, id) AS debut_year_change
    FROM cleaned_artists
    WINDOW w_country_is_verified AS (PARTITION BY country, is_verified), w_all_by_debut_year_ties AS (ORDER BY debut_year DESC)
),
enriched AS (
    -- Derive helper columns
    SELECT
        *,
        LEAST(debut_year_change, debut_year_avg_6_prior) AS least_debut_year_change_debut_year_avg_6_prior
    FROM sequenced
    WHERE is_verified = TRUE
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        debut_year_distance,
        second_debut_year,
        country_is_verified_sum_debut_year_distance,
        seq_no,
        second_debut_year_distance,
        debut_year_distance_cume_dist,
        running_debut_year_distance,
        least_debut_year_change_debut_year_avg_6_prior,
        ROUND(debut_year_distance - AVG(debut_year_distance) OVER (PARTITION BY is_verified), 2) AS debut_year_distance_vs_is_verified_avg,
        SUM(second_debut_year_distance_2) OVER (ORDER BY least_debut_year_change_debut_year_avg_6_prior ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_second_debut_year_distance_2,
        DENSE_RANK() OVER w_all_by_least_debut_year_chan_2 AS least_debut_year_change_debut_year_avg_6_prior_d,
        ROUND(second_debut_year_distance / NULLIF(SUM(second_debut_year_distance) OVER (PARTITION BY is_verified), 0), 4) AS second_debut_year_distance_share_of_is_verified,
        LAST_VALUE(debut_year_cume_dist) IGNORE NULLS OVER (ORDER BY least_debut_year_change_debut_year_avg_6_prior ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_debut_year_cume_dist,
        FIRST_VALUE(running_debut_year_distance) OVER (PARTITION BY is_verified, seq_no ORDER BY debut_year_avg_6_prior, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_running_debut_year_distance,
        LAG(debut_year_avg_6_prior) OVER (PARTITION BY is_verified ORDER BY debut_year_avg_6_prior, id) AS prev_debut_year_avg_6_prior,
        ROUND(debut_year_avg_6_prior - AVG(debut_year_avg_6_prior) OVER (PARTITION BY is_verified), 2) AS debut_year_avg_6_prior_vs_is_verified_avg
    FROM enriched
    WINDOW w_all_by_least_debut_year_chan_2 AS (ORDER BY least_debut_year_change_debut_year_avg_6_prior ASC, id)
)
SELECT
    id,
    debut_year_distance,
    second_debut_year,
    country_is_verified_sum_debut_year_distance,
    second_debut_year_distance,
    debut_year_distance_cume_dist,
    running_debut_year_distance,
    least_debut_year_change_debut_year_avg_6_prior,
    debut_year_distance_vs_is_verified_avg,
    least_debut_year_change_debut_year_avg_6_prior_d,
    debut_year_avg_6_prior_vs_is_verified_avg,
    ROUND(second_debut_year / NULLIF(debut_year_distance, 0), 3) AS second_debut_year_per_debut_year_distance
FROM with_windows
WHERE country_is_verified_sum_debut_year_distance > 8092
ORDER BY country_is_verified_sum_debut_year_distance DESC, id
LIMIT 20
