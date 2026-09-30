WITH base_listeners AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        username,
        country,
        signup_date,
        birth_year,
        interests,
        DATE_FORMAT(signup_date, 'yyyy-MM-dd') AS signup_date_label,
        CASE WHEN country IN ('BR', 'TH') THEN 'group_a' WHEN country = 'JP' THEN 'group_b' ELSE 'other' END AS country_group,
        DATEDIFF(DATE '2025-12-31', signup_date) AS days_since_signup_date
    FROM listeners
    WHERE signup_date BETWEEN DATE '2024-08-02' AND DATE '2025-05-05'
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY country_group, DATE_TRUNC('MONTH', signup_date)) AS rows_in_country_group_signup_date_month,
        ROUND(AVG(days_since_signup_date) OVER (ORDER BY signup_date, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS days_since_signup_date_avg_3,
        DENSE_RANK() OVER (ORDER BY signup_date) AS signup_date_dense_rank
    FROM base_listeners
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(CUME_DIST() OVER (PARTITION BY country ORDER BY rows_in_country_group_signup_date_month), 3) AS rows_in_country_group_signup_date_month_cume_dis,
        MAX(birth_year) OVER (PARTITION BY country_group ORDER BY birth_year, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS birth_year_max_2,
        NTILE(5) OVER (PARTITION BY signup_date_dense_rank, country ORDER BY days_since_signup_date_avg_3 NULLS LAST, id) AS days_since_signup_date_avg_3_quintile,
        NTILE(5) OVER (PARTITION BY country_group ORDER BY birth_year, id) AS birth_year_quintile
    FROM ranked
),
with_trends_2 AS (
    -- Previous / next values per key
    SELECT
        *,
        LAG(days_since_signup_date_avg_3, 3) OVER (PARTITION BY birth_year_quintile ORDER BY days_since_signup_date_avg_3_quintile, id) AS prev3_days_since_signup_date_avg_3,
        MAX(days_since_signup_date_avg_3) OVER (PARTITION BY birth_year_quintile ORDER BY birth_year DESC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS days_since_signup_date_avg_3_max_6_prior,
        LAST_VALUE(birth_year_max_2) IGNORE NULLS OVER (PARTITION BY birth_year_quintile ORDER BY birth_year DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_birth_year_max_2
    FROM with_trends
),
totals AS (
    -- Summarise per group
    SELECT
        birth_year_quintile,
        DATE_FORMAT(signup_date, 'yyyy-MM') AS signup_date_ym,
        ROUND(SUM(days_since_signup_date_avg_3_max_6_prior), 2) AS total_days_since_signup_date_avg_3_max_6_prior,
        ROUND(AVG(days_since_signup_date_avg_3_quintile), 2) AS avg_days_since_signup_date_avg_3_quintile
    FROM with_trends_2
    GROUP BY birth_year_quintile, DATE_FORMAT(signup_date, 'yyyy-MM')
),
qualified AS (
    -- Compare against the overall max
    SELECT *
    FROM totals
    WHERE birth_year_quintile < (SELECT MAX(birth_year_quintile) FROM totals)
),
labeled AS (
    -- Label and score rows
    SELECT
        *,
        SUBSTRING(TRIM(signup_date_ym), 1, 3) AS signup_date_ym_prefix,
        SUBSTRING(TRIM(signup_date_ym), 1, 4) AS signup_date_ym_prefix_2,
        COALESCE(birth_year_quintile, 0) AS birth_year_quintile_filled
    FROM qualified
)
SELECT
    birth_year_quintile,
    signup_date_ym,
    total_days_since_signup_date_avg_3_max_6_prior,
    avg_days_since_signup_date_avg_3_quintile,
    signup_date_ym_prefix_2,
    birth_year_quintile_filled,
    CONCAT_WS('-', signup_date_ym, signup_date_ym_prefix) AS signup_date_ym_signup_date_ym_prefix_label
FROM labeled
ORDER BY birth_year_quintile
