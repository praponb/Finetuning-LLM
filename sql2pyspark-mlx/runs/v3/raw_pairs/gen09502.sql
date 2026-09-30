SELECT
    id,
    status,
    paid_at,
    amount
FROM payments
WHERE order_id IN (12, 18, 22, 30) AND status <> 'REFUNDED'
ORDER BY amount, 1 DESC
