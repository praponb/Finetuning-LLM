SELECT
    id,
    amount,
    order_id,
    status
FROM payments
WHERE amount >= (SELECT AVG(amount) FROM payments)
ORDER BY order_id DESC, amount DESC, id
LIMIT 10;
