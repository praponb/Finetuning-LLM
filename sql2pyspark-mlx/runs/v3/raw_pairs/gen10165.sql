SELECT
    id,
    order_id,
    amount,
    SUM(amount) OVER (PARTITION BY order_id) AS order_id_sum_amount,
    MIN(amount) OVER (PARTITION BY order_id) AS order_id_min_amount
FROM payments
WHERE status = 'FAILED';
