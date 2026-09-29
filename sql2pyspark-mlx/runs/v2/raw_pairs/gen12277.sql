SELECT
    id,
    amount,
    paid_at,
    order_id
FROM payments
WHERE amount < (SELECT MIN(amount) FROM payments WHERE order_id IN (7, 29, 37, 38))
