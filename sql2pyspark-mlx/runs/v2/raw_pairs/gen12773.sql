SELECT
    id,
    country,
    signup_date,
    credit_limit,
    SUM(credit_limit) OVER (PARTITION BY country ORDER BY signup_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_credit_limit
FROM customers
WHERE signup_date > '2025-05-01'
