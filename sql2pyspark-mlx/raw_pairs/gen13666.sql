SELECT
    id,
    method,
    paid_at,
    amount,
    status,
    amount - LAG(amount, 1) OVER (PARTITION BY method ORDER BY paid_at, id) AS amount_change,
    LAG(amount, 2) OVER (PARTITION BY status ORDER BY paid_at, id) AS prev_amount
FROM payments;
