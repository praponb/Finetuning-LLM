SELECT p.id, p.method, p.order_id FROM payments p WHERE p.order_id IN (SELECT id FROM orders WHERE store_id IS NULL) ORDER BY method ASC, id DESC;
