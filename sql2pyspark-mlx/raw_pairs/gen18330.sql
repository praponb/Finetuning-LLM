SELECT
    id,
    amount,
    paid_at,
    status
FROM payments
WHERE amount >= (SELECT MAX(amount) FROM payments WHERE order_id IS NULL)
ORDER BY id DESC
