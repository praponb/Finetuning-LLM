SELECT
    id,
    order_id,
    amount,
    MIN(amount) OVER (PARTITION BY order_id) AS order_id_min_amount
FROM payments
