SELECT
    id,
    paid_at,
    amount,
    order_id,
    amount - LAG(amount, 1) OVER (ORDER BY paid_at, id) AS amount_change,
    ROUND(amount / SUM(amount) OVER (PARTITION BY order_id), 3) AS amount_share
FROM payments
WHERE paid_at >= '2023-10-01'
