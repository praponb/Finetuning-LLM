SELECT
    id,
    amount,
    order_id,
    status
FROM payments
WHERE amount > (SELECT MAX(amount) FROM payments)
ORDER BY order_id DESC, amount DESC
