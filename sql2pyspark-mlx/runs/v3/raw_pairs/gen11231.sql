SELECT id, amount
FROM payments
WHERE order_id IN (4, 25) AND order_id IN (5, 19, 24, 32)
ORDER BY id DESC;
