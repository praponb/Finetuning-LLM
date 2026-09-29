WITH base AS (
    -- Scope the listeners rows
    SELECT
        id,
        username,
        plan,
        country,
        signup_date,
        birth_year,
        interests,
        DATEDIFF(DATE '2025-12-31', signup_date) AS days_since_signup_date,
        DATE_TRUNC('MONTH', signup_date) AS signup_date_month
    FROM listeners
    WHERE birth_year < 1994
      AND signup_date < DATE '2024-12-21'
),
enriched AS (
    -- Add calculated columns
    SELECT
        id,
        username,
        country,
        signup_date,
        birth_year,
        days_since_signup_date,
        signup_date_month,
        DATEDIFF(DATE '2025-06-30', signup_date_month) AS days_since_signup_date_month,
        LENGTH(TRIM(username)) AS username_len
    FROM base
),
latest_per_key AS (
    -- Keep the latest row per country
    SELECT
        id,
        username,
        country,
        signup_date,
        birth_year,
        days_since_signup_date,
        signup_date_month,
        days_since_signup_date_month,
        username_len
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY country ORDER BY days_since_signup_date_month DESC, id DESC) AS row_num
        FROM enriched
    ) ranked
    WHERE row_num = 1
)
SELECT id, measure, amount
FROM (
    SELECT id, days_since_signup_date_month, username_len, days_since_signup_date, birth_year
    FROM latest_per_key
) src
UNPIVOT (
    amount FOR measure IN (days_since_signup_date_month, username_len, days_since_signup_date, birth_year)
) unp
