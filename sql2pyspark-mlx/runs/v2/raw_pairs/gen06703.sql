SELECT o.id, o.discount, o.store_id, o.amount FROM orders o WHERE o.store_id IN (22, 28) AND EXISTS (SELECT 1 FROM payments p WHERE p.order_id = o.id) ORDER BY amount DESC;
