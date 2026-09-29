SELECT
    id,
    amount,
    order_id,
    paid_at
FROM payments
WHERE order_id IS NOT NULL
