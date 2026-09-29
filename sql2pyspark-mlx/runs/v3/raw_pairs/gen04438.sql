SELECT
    id,
    order_id,
    paid_at,
    amount,
    LEAD(amount, 2) OVER (PARTITION BY order_id ORDER BY paid_at, id) next_amount
FROM payments
WHERE status IN ('FAILED', 'REFUNDED')
ORDER BY paid_at DESC;
