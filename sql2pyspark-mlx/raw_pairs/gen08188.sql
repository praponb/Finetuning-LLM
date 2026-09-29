SELECT
    id,
    city,
    signup_date,
    credit_limit,
    SUM(credit_limit) OVER (PARTITION BY city ORDER BY signup_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_credit_limit
FROM customers
ORDER BY credit_limit DESC;
