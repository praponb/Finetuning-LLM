WITH cleaned_artists AS (
    -- Filter artists
    SELECT
        id,
        country,
        debut_year,
        is_verified,
        COALESCE(debut_year, 0) AS debut_year_filled
    FROM artists
    WHERE genre = 'Pop'
),
tagged AS (
    -- Add calculated columns
    SELECT *, INITCAP(TRIM(country)) AS country_clean
    FROM cleaned_artists
),
derived AS (
    -- Label and score rows
    SELECT
        *,
        CASE WHEN country IN ('JP', 'KR') THEN 'group_a' WHEN country = 'TH' THEN 'group_b' ELSE 'other' END AS country_group,
        GREATEST(debut_year, debut_year_filled) AS greatest_debut_year_debut_year_filled,
        ROUND(debut_year_filled / NULLIF(debut_year, 0), 3) AS debut_year_filled_per_debut_year
    FROM tagged
    WHERE debut_year_filled < 2013
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        MIN(debut_year) OVER (PARTITION BY is_verified, country_group) AS is_verified_country_group_min_debut_year,
        COUNT(*) OVER (PARTITION BY is_verified, country_group) AS rows_in_is_verified_country_group,
        DENSE_RANK() OVER (PARTITION BY country_group ORDER BY debut_year DESC NULLS FIRST) AS debut_year_dense_rank,
        SIZE(COLLECT_SET(country) OVER (PARTITION BY is_verified, country_group)) AS distinct_country_in_is_verified_country_group,
        SUM(debut_year) OVER w_is_verified_country_group AS is_verified_country_group_sum_debut_year
    FROM derived
    WINDOW w_is_verified_country_group AS (PARTITION BY is_verified, country_group)
),
per_group AS (
    -- Aggregate with conditional metrics
    SELECT
        distinct_country_in_is_verified_country_group,
        debut_year_dense_rank,
        country_group,
        SUM(rows_in_is_verified_country_group) FILTER (WHERE is_verified = TRUE) AS rows_in_is_verified_country_group_is_verified_tr,
        SUM(debut_year_dense_rank) AS total_debut_year_dense_rank,
        SUM(CASE WHEN is_verified = TRUE THEN 1 ELSE 0 END) AS n_is_verified_true,
        SUM(CASE WHEN country IS NOT NULL THEN 1 ELSE 0 END) AS n_country_is_not_null
    FROM with_windows
    WHERE UPPER(TRIM(country_clean)) LIKE 'T%'
    GROUP BY distinct_country_in_is_verified_country_group, debut_year_dense_rank, country_group
    HAVING MAX(rows_in_is_verified_country_group) > 3
)
-- Subtotals with GROUPING SETS
SELECT
    distinct_country_in_is_verified_country_group,
    n_is_verified_true,
    COALESCE(country_group, 'ALL') AS country_group,
    ROUND(AVG(rows_in_is_verified_country_group_is_verified_tr), 2) AS avg_rows_in_is_verified_country_group_is_verifie,
    PERCENTILE_APPROX(n_country_is_not_null, 0.5) AS median_n_country_is_not_null
FROM per_group
GROUP BY GROUPING SETS ((distinct_country_in_is_verified_country_group, n_is_verified_true, country_group), (distinct_country_in_is_verified_country_group), ())
ORDER BY distinct_country_in_is_verified_country_group NULLS LAST, n_is_verified_true NULLS LAST, country_group NULLS LAST
