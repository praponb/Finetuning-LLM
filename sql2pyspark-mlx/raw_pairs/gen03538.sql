SELECT
    id,
    status,
    paid_at,
    order_id,
    amount,
    DENSE_RANK() OVER (PARTITION BY status ORDER BY paid_at) AS dense_rnk,
    ROUND(AVG(amount) OVER (PARTITION BY order_id ORDER BY paid_at, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS amount_moving_avg_4
FROM payments;
