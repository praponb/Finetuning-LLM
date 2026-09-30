SELECT
    id,
    method,
    status,
    paid_at,
    amount,
    order_id,
    LAG(amount, 1, 0) OVER (PARTITION BY method, status ORDER BY paid_at, id) AS prev_amount,
    SUM(amount) OVER (PARTITION BY order_id) AS order_id_sum_amount
FROM payments;
