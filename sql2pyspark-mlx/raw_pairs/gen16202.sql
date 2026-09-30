SELECT
    id,
    email,
    age,
    city,
    credit_limit + age AS credit_limit_plus_age,
    CASE WHEN segment = 'Consumer' THEN 1 ELSE 0 END AS is_consumer
FROM customers
ORDER BY id DESC
