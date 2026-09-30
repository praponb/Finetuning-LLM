SELECT
    id,
    segment,
    signup_date,
    credit_limit,
    country,
    credit_limit - LAG(credit_limit, 1) OVER (PARTITION BY segment ORDER BY signup_date, id) AS credit_limit_change,
    COUNT(*) OVER (PARTITION BY country) AS country_count
FROM customers
WHERE country <> 'UK';
