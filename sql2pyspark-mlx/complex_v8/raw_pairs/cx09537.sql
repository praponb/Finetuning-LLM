WITH base_artists AS (
    -- Scope the artists rows
    SELECT
        id,
        genre,
        country,
        debut_year,
        is_verified,
        CASE WHEN country IN ('KR', 'JP') THEN 'group_a' WHEN country = 'BR' THEN 'group_b' ELSE 'other' END AS country_group
    FROM artists
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        *,
        MAX(debut_year) OVER (PARTITION BY country, genre ORDER BY debut_year, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_debut_year_so_far,
        ROUND(debut_year - AVG(debut_year) OVER (PARTITION BY country_group, genre), 2) AS debut_year_vs_country_group_genre_avg,
        ROUND(AVG(debut_year) OVER w_country_is_verified, 2) AS country_is_verified_avg_debut_year,
        LAG(debut_year, 1, 0) OVER (PARTITION BY country_group ORDER BY debut_year, id) AS prev_debut_year,
        ROUND(CUME_DIST() OVER (ORDER BY debut_year, id), 3) AS debut_year_cume_dist,
        ROUND(debut_year / NULLIF(SUM(debut_year) OVER w_country_group_genre, 0), 4) AS debut_year_share_of_country_group_genre,
        ROUND(AVG(debut_year) OVER (ORDER BY debut_year ASC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS debut_year_avg_1_prior,
        SIZE(COLLECT_SET(country_group) OVER w_country_is_verified) AS distinct_country_group_in_country_is_verified,
        NTH_VALUE(debut_year, 2) OVER (PARTITION BY country_group ORDER BY debut_year, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_debut_year,
        NTH_VALUE(debut_year, 2) OVER (ORDER BY debut_year, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_debut_year_2,
        LAG(debut_year) OVER (PARTITION BY country_group ORDER BY debut_year, id) AS prev_debut_year_2,
        MAX(debut_year) OVER (PARTITION BY country, is_verified) AS country_is_verified_max_debut_year
    FROM base_artists
    WINDOW w_country_is_verified AS (PARTITION BY country, is_verified), w_country_group_genre AS (PARTITION BY country_group, genre)
),
tagged AS (
    -- Label and score rows
    SELECT
        *,
        IF(is_verified, 'yes', 'no') AS is_verified_label,
        ROUND(debut_year_avg_1_prior * 0.9, 2) AS debut_year_avg_1_prior_adj,
        ABS(second_debut_year_2 - 1997) AS second_debut_year_2_distance
    FROM with_windows
)
SELECT
    id,
    country,
    debut_year,
    is_verified,
    max_debut_year_so_far,
    country_is_verified_avg_debut_year,
    debut_year_cume_dist,
    debut_year_avg_1_prior,
    second_debut_year_2,
    prev_debut_year_2,
    country_is_verified_max_debut_year,
    second_debut_year_2_distance,
    LOWER(TRIM(country_group)) AS country_group_clean,
    LOWER(REGEXP_REPLACE(TRIM(country), ' ', '_')) AS country_slug
FROM tagged
ORDER BY prev_debut_year_2, id
LIMIT 20
