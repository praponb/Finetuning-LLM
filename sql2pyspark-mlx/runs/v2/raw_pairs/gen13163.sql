SELECT
    id,
    status,
    paid_at,
    amount,
    amount - LAG(amount, 1) OVER (PARTITION BY status ORDER BY paid_at, id) AS amount_change
FROM payments
WHERE order_id IS NOT NULL;
