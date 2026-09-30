WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        username,
        country,
        signup_date,
        interests
    FROM listeners
    WHERE plan IN ('premium', 'free', 'family')
      AND birth_year <= 1988
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        username,
        country,
        signup_date,
        interests,
        FIRST_VALUE(signup_date) OVER (PARTITION BY country, DATE_TRUNC('MONTH', signup_date) ORDER BY signup_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_signup_date,
        LEAD(signup_date) OVER w_signup_date_month_by_signup AS next_signup_date,
        NTH_VALUE(signup_date, 2) OVER (PARTITION BY country ORDER BY signup_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_signup_date,
        FIRST_VALUE(signup_date) IGNORE NULLS OVER (PARTITION BY DATE_TRUNC('MONTH', signup_date) ORDER BY signup_date ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_signup_date,
        RANK() OVER w_country_signup_date_month_by_2 AS signup_date_rank,
        NTILE(3) OVER w_country_signup_date_month_by_2 AS signup_date_tercile,
        ROUND(PERCENT_RANK() OVER w_signup_date_month_by_signup_3, 3) AS signup_date_percent_rank,
        DATEDIFF(signup_date, LAG(signup_date) OVER (PARTITION BY DATE_TRUNC('MONTH', signup_date) ORDER BY signup_date ASC, id)) AS days_since_prev_signup_date,
        NTH_VALUE(signup_date, 2) OVER (PARTITION BY DATE_TRUNC('MONTH', signup_date) ORDER BY signup_date ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_signup_date_2
    FROM base
    WINDOW w_signup_date_month_by_signup AS (PARTITION BY DATE_TRUNC('MONTH', signup_date) ORDER BY signup_date NULLS LAST, id), w_country_signup_date_month_by_2 AS (PARTITION BY country, DATE_TRUNC('MONTH', signup_date) ORDER BY signup_date, id), w_signup_date_month_by_signup_3 AS (PARTITION BY DATE_TRUNC('MONTH', signup_date) ORDER BY signup_date ASC)
),
windowed AS (
    -- Previous / next values per key
    SELECT
        id,
        username,
        country,
        interests,
        first_signup_date,
        next_signup_date,
        first_known_signup_date,
        signup_date_rank,
        signup_date_tercile,
        signup_date_percent_rank,
        days_since_prev_signup_date,
        second_signup_date_2,
        COUNT(*) OVER w_second_signup_date_month AS rows_in_second_signup_date_month,
        SIZE(COLLECT_SET(country) OVER (PARTITION BY MONTH(second_signup_date))) AS distinct_country_in_second_signup_date_month,
        MIN(days_since_prev_signup_date) OVER (PARTITION BY MONTH(second_signup_date)) AS second_signup_date_month_min_days_since_prev_sig,
        MIN(signup_date_rank) OVER (PARTITION BY signup_date_tercile, signup_date_rank ORDER BY signup_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_signup_date_rank_so_far
    FROM metrics
    WINDOW w_second_signup_date_month AS (PARTITION BY MONTH(second_signup_date))
)
-- Final projection
SELECT
    id,
    username,
    country,
    interests,
    first_signup_date,
    next_signup_date,
    first_known_signup_date,
    signup_date_rank,
    signup_date_tercile,
    signup_date_percent_rank,
    days_since_prev_signup_date,
    second_signup_date_2,
    distinct_country_in_second_signup_date_month,
    second_signup_date_month_min_days_since_prev_sig,
    min_signup_date_rank_so_far,
    COALESCE(country, 'UNKNOWN') AS country_or_unknown
FROM windowed
ORDER BY days_since_prev_signup_date DESC, signup_date_rank DESC
