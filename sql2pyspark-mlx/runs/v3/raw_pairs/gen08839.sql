SELECT o.id, o.store_id FROM orders o WHERE EXISTS (SELECT 1 FROM payments p WHERE p.order_id = o.id AND p.order_id IS NOT NULL);
