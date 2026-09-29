SELECT
    id,
    order_id,
    paid_at,
    amount,
    ROUND(AVG(amount) OVER (PARTITION BY order_id ORDER BY paid_at, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS amount_moving_avg_3
FROM payments
ORDER BY amount_moving_avg_3 DESC, order_id;
