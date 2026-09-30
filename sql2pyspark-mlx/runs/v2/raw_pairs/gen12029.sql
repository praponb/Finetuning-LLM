SELECT p.id, p.amount
FROM payments p
WHERE p.order_id IN (SELECT id FROM orders WHERE discount > 0.32)
ORDER BY p.id;
