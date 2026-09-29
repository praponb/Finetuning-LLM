SELECT
    *,
    DENSE_RANK() OVER (PARTITION BY order_id, status ORDER BY amount) AS amount_dense_rank,
    ROUND(amount / SUM(amount) OVER (PARTITION BY method), 4) AS amount_share,
    AVG(amount) OVER (ORDER BY paid_at, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS amount_avg_2
FROM payments
