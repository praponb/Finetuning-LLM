WITH cleaned_artists AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        genre,
        country,
        debut_year,
        is_verified,
        CASE WHEN debut_year >= 2016 THEN 'A' WHEN debut_year >= 1996 THEN 'B' ELSE 'C' END AS debut_year_tier,
        CONCAT(country, '#', CAST(debut_year AS STRING)) AS country_ref
    FROM artists
    WHERE debut_year < 2020
      AND genre IN ('Classical', 'Pop', 'HipHop')
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        id,
        country,
        debut_year_tier,
        country_ref,
        ROUND(PERCENT_RANK() OVER (PARTITION BY is_verified ORDER BY debut_year DESC, id), 3) AS debut_year_percent_rank,
        ROUND(debut_year - AVG(debut_year) OVER w_country_is_verified, 2) AS debut_year_vs_country_is_verified_avg,
        ROUND(CUME_DIST() OVER w_is_verified_by_debut_year_ti, 3) AS debut_year_cume_dist,
        ROUND(debut_year / NULLIF(SUM(debut_year) OVER (PARTITION BY country, is_verified), 0), 4) AS debut_year_share_of_country_is_verified,
        MAX(debut_year) OVER (PARTITION BY is_verified ORDER BY debut_year DESC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS debut_year_max_1_prior,
        MIN(debut_year) OVER (PARTITION BY is_verified ORDER BY debut_year DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_debut_year_so_far,
        SIZE(COLLECT_SET(debut_year_tier) OVER (PARTITION BY country, is_verified)) AS distinct_debut_year_tier_in_country_is_verified,
        LAG(debut_year) OVER w_is_verified_by_debut_year AS prev_debut_year,
        FIRST_VALUE(debut_year) OVER (PARTITION BY is_verified ORDER BY debut_year DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_debut_year,
        LAST_VALUE(debut_year) OVER (PARTITION BY is_verified ORDER BY debut_year DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_debut_year,
        NTH_VALUE(debut_year, 2) OVER (ORDER BY debut_year, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_debut_year,
        NTH_VALUE(debut_year, 2) OVER (PARTITION BY is_verified ORDER BY debut_year DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_debut_year_2
    FROM cleaned_artists
    WINDOW w_country_is_verified AS (PARTITION BY country, is_verified), w_is_verified_by_debut_year_ti AS (PARTITION BY is_verified ORDER BY debut_year DESC), w_is_verified_by_debut_year AS (PARTITION BY is_verified ORDER BY debut_year DESC, id)
)
SELECT
    id,
    country,
    debut_year_tier,
    country_ref,
    debut_year_vs_country_is_verified_avg,
    debut_year_share_of_country_is_verified,
    min_debut_year_so_far,
    first_debut_year,
    last_debut_year,
    second_debut_year,
    last_debut_year - second_debut_year_2 AS last_debut_year_minus_second_debut_year_2,
    LOWER(REGEXP_REPLACE(TRIM(country_ref), ' ', '_')) AS country_ref_slug
FROM windowed
WHERE debut_year_cume_dist > 0.1
ORDER BY id DESC, first_debut_year
