SELECT
    id,
    method,
    paid_at,
    amount,
    order_id,
    amount - LAG(amount, 1) OVER (PARTITION BY method ORDER BY paid_at, id) amount_change,
    SUM(amount) OVER (PARTITION BY order_id) order_id_sum_amount
FROM payments;
