SELECT
    id,
    country,
    signup_date,
    credit_limit,
    ROUND(AVG(credit_limit) OVER (PARTITION BY country ORDER BY signup_date, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS credit_limit_moving_avg_7
FROM customers
