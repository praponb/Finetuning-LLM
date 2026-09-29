SELECT
    id,
    method,
    amount,
    order_id,
    ROUND(amount / SUM(amount) OVER (PARTITION BY method), 3) AS amount_share,
    LAST_VALUE(order_id) OVER (PARTITION BY method ORDER BY amount DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_order_id
FROM payments;
