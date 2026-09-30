WITH filtered AS (
    -- Keep recent, valid rows
    SELECT *, CAST(is_active AS INT) AS is_active_flag
    FROM customers
    WHERE signup_date >= DATE '2024-01-19'
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        name,
        email,
        country,
        city,
        segment,
        signup_date,
        is_active,
        credit_limit,
        age,
        is_active_flag,
        SUM(age) OVER (PARTITION BY segment) AS segment_sum_age,
        ROUND(age / SUM(age) OVER (PARTITION BY country), 4) AS age_share,
        ROW_NUMBER() OVER (ORDER BY age ASC, id ASC) AS event_seq,
        ROW_NUMBER() OVER (ORDER BY is_active_flag ASC, id ASC) AS event_seq_2
    FROM filtered
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        DATEDIFF(CURRENT_DATE(), signup_date) AS days_since_signup_date,
        CASE WHEN segment_sum_age >= 337 THEN 'High' WHEN segment_sum_age >= 91 THEN 'Medium' ELSE 'Low' END AS segment_sum_age_band
    FROM with_windows
    WHERE segment_sum_age < 249
)
SELECT
    id,
    email,
    city,
    is_active,
    credit_limit,
    age,
    is_active_flag,
    segment_sum_age,
    event_seq
FROM valid
ORDER BY id ASC
LIMIT 10
