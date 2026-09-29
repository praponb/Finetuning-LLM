SELECT p.id, p.order_id FROM payments p WHERE p.order_id IS NOT NULL AND p.order_id IN (SELECT id FROM orders WHERE channel IS NULL);
