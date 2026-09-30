SELECT o.id, o.status, o.amount
FROM orders o
WHERE o.id IN (SELECT order_id FROM payments WHERE amount IS NULL);
