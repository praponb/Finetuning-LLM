WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        name,
        email,
        country,
        city,
        segment,
        signup_date,
        is_active,
        age,
        SPLIT(LOWER(email), '@')[0] AS email_user
    FROM customers
    WHERE city IS NOT NULL
)
-- Running and moving aggregates
SELECT
    *,
    NTILE(10) OVER (PARTITION BY segment, country ORDER BY age, id) AS age_bucket,
    LEAD(age, 1) OVER (PARTITION BY segment ORDER BY age, id) AS next_age,
    MAX(age) OVER (PARTITION BY segment, city) AS segment_max_age,
    LAST_VALUE(city) IGNORE NULLS OVER (PARTITION BY city ORDER BY age, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_city,
    ROW_NUMBER() OVER (PARTITION BY city, country ORDER BY signup_date DESC, id) AS row_seq
FROM base;
