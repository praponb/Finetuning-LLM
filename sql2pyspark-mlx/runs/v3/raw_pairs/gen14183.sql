SELECT
    id,
    order_id,
    amount,
    AVG(amount) OVER (PARTITION BY order_id) order_id_avg_amount
FROM payments
