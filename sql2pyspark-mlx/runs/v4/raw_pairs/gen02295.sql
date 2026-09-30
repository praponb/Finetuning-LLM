SELECT o.id, o.status
FROM orders o
WHERE o.discount <= 0.11 AND EXISTS (SELECT 1 FROM payments p WHERE p.order_id = o.id AND p.order_id IS NULL);
