SELECT
    id,
    country,
    city,
    signup_date,
    is_active,
    credit_limit,
    MAX(age) OVER w_running AS last3_max_age,
    COUNT(age) OVER w_running AS last3_count_age,
    SUM(age) OVER w_running AS last3_sum_age
FROM customers
WHERE email LIKE '%@mail.co.th'
WINDOW w_running AS (PARTITION BY country ORDER BY age, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
