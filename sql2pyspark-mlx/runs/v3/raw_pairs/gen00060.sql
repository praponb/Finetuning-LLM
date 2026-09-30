SELECT
    id,
    country,
    credit_limit,
    SUM(credit_limit) OVER (PARTITION BY country) AS country_sum_credit_limit
FROM customers
WHERE LOWER(TRIM(name)) = 'grace kim';
