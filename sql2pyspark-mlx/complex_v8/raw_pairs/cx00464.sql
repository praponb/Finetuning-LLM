WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        username,
        plan,
        country,
        signup_date,
        birth_year,
        interests,
        DATE_ADD(signup_date, 7) AS signup_date_plus_7d,
        DATE_ADD(signup_date, 90) AS signup_date_plus_90d,
        CONCAT(username, '#', CAST(id AS STRING)) AS username_ref
    FROM listeners
    WHERE signup_date IS NOT NULL
      AND plan = 'free'
),
plan_stats AS (
    -- Max birth_year per plan
    SELECT plan, MAX(birth_year) AS max_birth_year
    FROM scoped
    GROUP BY plan
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_birth_year,
        ROUND(s.birth_year - g.max_birth_year, 2) AS birth_year_minus_max
    FROM scoped s
    JOIN plan_stats g ON g.plan = s.plan
    WHERE s.birth_year = g.max_birth_year
),
per_group AS (
    -- Group-level totals
    SELECT
        country,
        DATE_TRUNC('MONTH', signup_date_plus_90d) AS signup_date_plus_90d_month,
        DATE_FORMAT(signup_date_plus_7d, 'yyyy-MM') AS signup_date_plus_7d_ym,
        COUNT(*) AS cnt,
        MAX(birth_year_minus_max) AS max_birth_year_minus_max,
        COUNT(DISTINCT id) AS distinct_ids,
        SUM(CASE WHEN country IS NOT NULL THEN 1 ELSE 0 END) AS n_country_is_not_null,
        ROUND(AVG(birth_year), 2) AS avg_birth_year
    FROM above_average
    WHERE max_birth_year < 1996.0
    GROUP BY country, DATE_TRUNC('MONTH', signup_date_plus_90d), DATE_FORMAT(signup_date_plus_7d, 'yyyy-MM')
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        country,
        signup_date_plus_90d_month,
        signup_date_plus_7d_ym,
        max_birth_year_minus_max,
        distinct_ids,
        n_country_is_not_null,
        avg_birth_year,
        SIZE(COLLECT_SET(country) OVER (PARTITION BY distinct_ids)) AS distinct_country_in_distinct_ids,
        FIRST_VALUE(signup_date_plus_90d_month) IGNORE NULLS OVER (PARTITION BY distinct_ids ORDER BY distinct_ids, country, signup_date_plus_90d_month, signup_date_plus_7d_ym ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_signup_date_plus_90d_month,
        distinct_ids - LAG(distinct_ids) OVER (PARTITION BY n_country_is_not_null ORDER BY cnt DESC, country, signup_date_plus_90d_month, signup_date_plus_7d_ym) AS distinct_ids_change
    FROM per_group
),
without_matches AS (
    -- Compare against the overall max
    SELECT *
    FROM with_windows
    WHERE distinct_ids < (SELECT MAX(distinct_ids) FROM with_windows)
),
qualified AS (
    -- Compare against the overall avg
    SELECT *
    FROM without_matches
    WHERE distinct_ids > (SELECT AVG(distinct_ids) FROM without_matches)
)
-- Final projection
SELECT
    country,
    signup_date_plus_90d_month,
    signup_date_plus_7d_ym,
    max_birth_year_minus_max,
    distinct_ids,
    n_country_is_not_null,
    avg_birth_year,
    distinct_country_in_distinct_ids,
    first_known_signup_date_plus_90d_month,
    distinct_ids_change,
    CONCAT(signup_date_plus_7d_ym, '#', CAST(n_country_is_not_null AS STRING)) AS signup_date_plus_7d_ym_ref
FROM qualified
ORDER BY avg_birth_year DESC NULLS FIRST, distinct_ids
