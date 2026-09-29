WITH valid_listeners AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        username,
        country,
        signup_date,
        birth_year,
        ADD_MONTHS(signup_date, 12) AS signup_date_plus_12m,
        UPPER(TRIM(country)) AS country_clean
    FROM listeners
    WHERE birth_year > 1967
      AND SIZE(interests) >= 1
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        FIRST_VALUE(birth_year) OVER (ORDER BY signup_date_plus_12m ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_birth_year,
        COUNT(*) OVER (PARTITION BY MONTH(signup_date)) AS rows_in_signup_date_month,
        MIN(birth_year) OVER (PARTITION BY MONTH(signup_date)) AS signup_date_month_min_birth_year,
        SUM(birth_year) OVER w_signup_date_month AS signup_date_month_sum_birth_year,
        LAST_VALUE(signup_date_plus_12m) IGNORE NULLS OVER (ORDER BY signup_date_plus_12m ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_signup_date_plus_12m,
        ROUND(birth_year / NULLIF(SUM(birth_year) OVER (PARTITION BY MONTH(signup_date)), 0), 4) AS birth_year_share_of_signup_date_month,
        DATEDIFF(signup_date, LAG(signup_date) OVER (ORDER BY signup_date_plus_12m ASC, id)) AS days_since_prev_signup_date,
        FIRST_VALUE(signup_date) OVER (PARTITION BY YEAR(signup_date), country ORDER BY signup_date_plus_12m DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_signup_date,
        RANK() OVER (PARTITION BY country, DATE_TRUNC('MONTH', signup_date) ORDER BY birth_year DESC) AS birth_year_rank,
        LAST_VALUE(birth_year) OVER (ORDER BY signup_date_plus_12m ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_birth_year
    FROM valid_listeners
    WINDOW w_signup_date_month AS (PARTITION BY MONTH(signup_date))
)
-- Final projection
SELECT
    id,
    country,
    signup_date,
    birth_year,
    country_clean,
    first_birth_year,
    rows_in_signup_date_month,
    signup_date_month_min_birth_year,
    birth_year_share_of_signup_date_month,
    days_since_prev_signup_date,
    birth_year_rank,
    last_birth_year
FROM with_windows
WHERE birth_year_share_of_signup_date_month > 0.2
ORDER BY country, id DESC
