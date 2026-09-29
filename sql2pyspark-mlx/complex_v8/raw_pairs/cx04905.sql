WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        username,
        country,
        signup_date,
        birth_year,
        interests
    FROM listeners
    WHERE birth_year < 1981
),
labeled AS (
    -- Add calculated columns
    SELECT
        id,
        username,
        country,
        signup_date,
        birth_year,
        FLOOR(birth_year / 10) * 10 AS birth_year_bucket,
        DATEDIFF(DATE '2026-01-01', signup_date) AS days_since_signup_date
    FROM base
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        LAG(signup_date) OVER (ORDER BY days_since_signup_date, id) AS prev_signup_date,
        LEAD(signup_date) OVER (ORDER BY signup_date, id) AS next_signup_date
    FROM labeled
),
unique_rows AS (
    -- Keep the latest row per country
    SELECT
        id,
        username,
        country,
        signup_date,
        birth_year,
        birth_year_bucket,
        days_since_signup_date,
        prev_signup_date,
        next_signup_date
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY country ORDER BY next_signup_date DESC, id DESC) AS rn
        FROM windowed
    ) ranked
    WHERE rn = 1
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        username,
        country,
        days_since_signup_date,
        next_signup_date,
        ROUND(AVG(days_since_signup_date) OVER (PARTITION BY DATE_TRUNC('MONTH', signup_date) ORDER BY birth_year_bucket, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS days_since_signup_date_avg_1_prior,
        ROUND(days_since_signup_date - AVG(days_since_signup_date) OVER (PARTITION BY MONTH(prev_signup_date), MONTH(next_signup_date)), 2) AS days_since_signup_date_vs_prev_signup_date_month,
        ROUND(days_since_signup_date / NULLIF(SUM(days_since_signup_date) OVER w_prev_signup_date_month_next, 0), 4) AS days_since_signup_date_share_of_prev_signup_date,
        NTILE(5) OVER (PARTITION BY YEAR(prev_signup_date) ORDER BY next_signup_date ASC, id) AS next_signup_date_quintile,
        NTH_VALUE(prev_signup_date, 2) OVER (PARTITION BY YEAR(prev_signup_date) ORDER BY next_signup_date ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_prev_signup_date
    FROM unique_rows
    WINDOW w_prev_signup_date_month_next AS (PARTITION BY MONTH(prev_signup_date), MONTH(next_signup_date))
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, metric, value
FROM (
    SELECT id, days_since_signup_date_share_of_prev_signup_date, days_since_signup_date_avg_1_prior, days_since_signup_date_vs_prev_signup_date_month
    FROM sequenced
) src
UNPIVOT (
    value FOR metric IN (days_since_signup_date_share_of_prev_signup_date, days_since_signup_date_avg_1_prior, days_since_signup_date_vs_prev_signup_date_month)
) unp
ORDER BY id, metric
