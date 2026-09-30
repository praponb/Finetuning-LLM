SELECT o.id, o.amount
FROM orders o
WHERE o.id IN (SELECT order_id FROM payments)
ORDER BY id DESC;
