SELECT
    id,
    order_date,
    amount,
    ROUND(AVG(amount) OVER (ORDER BY order_date, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS amount_moving_avg_3
FROM orders;
