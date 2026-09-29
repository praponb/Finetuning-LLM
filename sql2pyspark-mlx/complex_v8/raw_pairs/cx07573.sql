WITH scoped_listeners AS (
    -- Scope the listeners rows
    SELECT
        id,
        username,
        signup_date,
        birth_year,
        DATEDIFF(DATE '2025-12-31', signup_date) AS days_since_signup_date,
        CONCAT_WS('-', username, country) AS username_country_label
    FROM listeners
    WHERE country IN ('JP', 'TH')
),
labeled AS (
    -- Add calculated columns
    SELECT
        *,
        TRUNC(signup_date, 'MM') AS signup_date_month_start,
        LENGTH(TRIM(username_country_label)) AS username_country_label_len,
        LENGTH(TRIM(username_country_label)) AS username_country_label_len_2
    FROM scoped_listeners
    WHERE birth_year > 1969
),
qualified AS (
    -- Compare against the overall min
    SELECT *
    FROM labeled
    WHERE days_since_signup_date > (SELECT MIN(days_since_signup_date) FROM labeled)
)
-- Final projection
SELECT
    id,
    username,
    birth_year,
    days_since_signup_date,
    signup_date_month_start,
    username_country_label_len
FROM qualified
