SELECT
    id,
    email,
    country,
    signup_date,
    is_active,
    credit_limit,
    age,
    LAST_VALUE(city) IGNORE NULLS OVER w_running AS last_known_city,
    COUNT(age) OVER (PARTITION BY segment) AS segment_count_age,
    NTILE(5) OVER (PARTITION BY segment ORDER BY age DESC, id) AS age_bucket
FROM customers
WINDOW w_running AS (PARTITION BY country ORDER BY credit_limit, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW);
