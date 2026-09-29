WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_name,
        genre,
        country,
        debut_year,
        is_verified,
        IF(is_verified, 'yes', 'no') AS is_verified_label,
        ABS(debut_year - 1997) AS debut_year_distance,
        LOWER(REGEXP_REPLACE(TRIM(country), ' ', '_')) AS country_slug
    FROM artists
    WHERE is_verified = TRUE
      AND debut_year > 1997
),
is_verified_label_stats AS (
    -- Max debut_year_distance per is_verified_label
    SELECT is_verified_label, MAX(debut_year_distance) AS max_debut_year_distance
    FROM prepared
    GROUP BY is_verified_label
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_debut_year_distance,
        ROUND(s.debut_year_distance - g.max_debut_year_distance, 2) AS debut_year_distance_minus_max
    FROM prepared s
    JOIN is_verified_label_stats g ON g.is_verified_label = s.is_verified_label
    WHERE s.debut_year_distance = g.max_debut_year_distance
),
per_group AS (
    -- Group-level totals
    SELECT
        country,
        genre,
        is_verified_label,
        SUM(CASE WHEN country IS NOT NULL THEN 1 ELSE 0 END) AS n_country_is_not_null,
        SUM(CASE WHEN country <> 'JP' THEN 1 ELSE 0 END) AS n_country_jp,
        SUM(debut_year_distance) AS total_debut_year_distance
    FROM above_average
    GROUP BY 1, 2, 3
    HAVING AVG(max_debut_year_distance) > 303.4
),
tagged AS (
    -- Label and score rows
    SELECT
        country,
        genre,
        is_verified_label,
        n_country_is_not_null,
        n_country_jp,
        total_debut_year_distance,
        CASE WHEN country IN ('US', 'JP') THEN 'group_a' WHEN country = 'TH' THEN 'group_b' ELSE 'other' END AS country_group
    FROM per_group
),
annotated AS (
    -- Label and score rows
    SELECT
        country,
        genre,
        is_verified_label,
        n_country_is_not_null,
        n_country_jp,
        total_debut_year_distance,
        country_group,
        UPPER(TRIM(country_group)) AS country_group_clean,
        LOWER(TRIM(country)) AS country_clean,
        ROUND(n_country_jp / NULLIF(n_country_is_not_null, 0), 3) AS n_country_jp_per_n_country_is_not_null
    FROM tagged
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        country,
        genre,
        is_verified_label,
        n_country_is_not_null,
        total_debut_year_distance,
        country_group_clean,
        LAST_VALUE(total_debut_year_distance) IGNORE NULLS OVER (PARTITION BY is_verified_label ORDER BY n_country_jp DESC, country, genre, is_verified_label ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_total_debut_year_distance,
        LAST_VALUE(n_country_is_not_null) IGNORE NULLS OVER (PARTITION BY is_verified_label ORDER BY n_country_jp DESC, country, genre, is_verified_label ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_n_country_is_not_null,
        MIN(total_debut_year_distance) OVER (PARTITION BY n_country_jp) AS n_country_jp_min_total_debut_year_distance,
        DENSE_RANK() OVER w_is_verified_label_by_n_count_2 AS n_country_jp_dense_rank,
        SUM(n_country_jp) OVER (PARTITION BY is_verified_label, genre ORDER BY total_debut_year_distance DESC, country, genre, is_verified_label ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS n_country_jp_sum_3_prior
    FROM annotated
    WINDOW w_is_verified_label_by_n_count_2 AS (PARTITION BY is_verified_label ORDER BY n_country_jp DESC)
)
SELECT country, genre, metric, reading
FROM (
    SELECT country, genre, n_country_jp_min_total_debut_year_distance, n_country_is_not_null
    FROM with_windows
) src
UNPIVOT (
    reading FOR metric IN (n_country_jp_min_total_debut_year_distance, n_country_is_not_null)
) unp
