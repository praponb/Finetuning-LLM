SELECT
    id,
    city,
    signup_date,
    credit_limit,
    segment,
    ROUND(AVG(credit_limit) OVER (PARTITION BY city ORDER BY signup_date, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS credit_limit_moving_avg_3,
    LAG(credit_limit, 1, 0) OVER (PARTITION BY segment, city ORDER BY signup_date, id) AS prev_credit_limit
FROM customers
WHERE city IN ('London', 'Tokyo')
