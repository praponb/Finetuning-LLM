SELECT p.id, p.method FROM payments p WHERE p.order_id IN (SELECT id FROM orders WHERE store_id IS NULL)
