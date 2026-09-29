SELECT
    id,
    city,
    credit_limit,
    MIN(credit_limit) OVER (PARTITION BY city) AS city_min_credit_limit
FROM customers;
