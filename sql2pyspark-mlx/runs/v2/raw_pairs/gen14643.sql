SELECT
    id,
    segment,
    signup_date,
    credit_limit,
    city,
    LAST_VALUE(credit_limit) OVER (PARTITION BY segment ORDER BY signup_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_credit_limit,
    ROUND(AVG(credit_limit) OVER (PARTITION BY city ORDER BY signup_date, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS credit_limit_moving_avg_3
FROM customers
WHERE country = 'DE';
