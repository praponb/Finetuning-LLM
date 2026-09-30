WITH filtered_listeners AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        username,
        country,
        signup_date,
        birth_year,
        interests,
        DATEDIFF(DATE '2025-12-31', signup_date) AS days_since_signup_date
    FROM listeners
    WHERE country = 'KR'
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        *,
        MIN(days_since_signup_date) OVER w_signup_date_month AS signup_date_month_min_days_since_signup_date,
        ROUND(CUME_DIST() OVER (PARTITION BY country ORDER BY birth_year, id), 3) AS birth_year_cume_dist,
        DENSE_RANK() OVER (PARTITION BY country ORDER BY birth_year ASC, id) AS birth_year_dense_rank,
        MAX(birth_year) OVER (PARTITION BY MONTH(signup_date)) AS signup_date_month_max_birth_year,
        RANK() OVER (ORDER BY signup_date ASC) AS signup_date_rank,
        FIRST_VALUE(signup_date) IGNORE NULLS OVER (ORDER BY signup_date ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_signup_date,
        NTILE(3) OVER (PARTITION BY country ORDER BY birth_year, id) AS birth_year_tercile,
        COUNT(*) OVER (PARTITION BY MONTH(signup_date)) AS rows_in_signup_date_month
    FROM filtered_listeners
    WINDOW w_signup_date_month AS (PARTITION BY MONTH(signup_date))
),
enriched AS (
    -- Add calculated columns
    SELECT
        *,
        UPPER(TRIM(country)) AS country_clean,
        ARRAY_JOIN(ARRAY_SORT(interests), ',') AS interests_text
    FROM with_trends
    WHERE rows_in_signup_date_month <= 17
),
with_trends_2 AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        LAG(birth_year_tercile, 3) OVER (PARTITION BY YEAR(first_known_signup_date) ORDER BY signup_date NULLS LAST, id) AS prev3_birth_year_tercile,
        NTILE(10) OVER w_all_by_signup_date_month_max AS signup_date_month_max_birth_year_decile,
        ROUND(AVG(days_since_signup_date) OVER (ORDER BY first_known_signup_date DESC NULLS FIRST, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS days_since_signup_date_avg_4,
        NTH_VALUE(birth_year_tercile, 2) OVER (ORDER BY signup_date_month_max_birth_year DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_birth_year_tercile
    FROM enriched
    WINDOW w_all_by_signup_date_month_max AS (ORDER BY signup_date_month_max_birth_year DESC NULLS FIRST, id)
)
-- Final projection
SELECT
    id,
    username,
    country,
    signup_date,
    birth_year,
    interests,
    days_since_signup_date,
    birth_year_cume_dist,
    signup_date_month_max_birth_year,
    signup_date_rank,
    first_known_signup_date,
    rows_in_signup_date_month,
    country_clean,
    interests_text,
    signup_date_month_max_birth_year_decile,
    second_birth_year_tercile,
    CASE WHEN country IN ('JP', 'TH') THEN 'group_a' WHEN country = 'UK' THEN 'group_b' ELSE 'other' END AS country_group,
    QUARTER(signup_date) AS signup_date_quarter
FROM with_trends_2
ORDER BY rows_in_signup_date_month
