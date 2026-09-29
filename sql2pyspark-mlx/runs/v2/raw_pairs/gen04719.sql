SELECT
    id,
    method,
    paid_at,
    amount,
    status,
    ROUND(AVG(amount) OVER (PARTITION BY method ORDER BY paid_at, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS amount_moving_avg_3,
    DENSE_RANK() OVER (PARTITION BY status ORDER BY amount) AS dense_rnk
FROM payments;
