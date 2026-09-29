SELECT
    id,
    status,
    order_date,
    amount,
    ROUND(AVG(amount) OVER (PARTITION BY status ORDER BY order_date, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS amount_moving_avg_4
FROM orders
WHERE channel IN ('store', 'web', 'mobile');
