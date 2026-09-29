SELECT
    id,
    order_id,
    method,
    amount,
    paid_at,
    status,
    LEAD(amount, 2) OVER (ORDER BY amount, id) AS next_amount,
    CUME_DIST() OVER (PARTITION BY method ORDER BY amount) AS amount_cume_dist
FROM payments;
