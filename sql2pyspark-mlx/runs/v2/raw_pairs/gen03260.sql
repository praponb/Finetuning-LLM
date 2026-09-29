SELECT
    id,
    order_id,
    paid_at,
    amount,
    ROUND(AVG(amount) OVER (PARTITION BY order_id ORDER BY paid_at, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) amount_moving_avg_4
FROM payments
