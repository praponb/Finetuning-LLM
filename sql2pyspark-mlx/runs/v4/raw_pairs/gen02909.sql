SELECT
    id,
    order_id,
    method,
    amount,
    paid_at,
    status,
    SUM(amount) OVER (ORDER BY amount ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_amount,
    LAG(amount, 1) OVER (PARTITION BY method, status ORDER BY amount ASC, id ASC) AS prev_amount,
    SUM(amount) OVER (PARTITION BY order_id) AS order_id_sum_amount
FROM payments
