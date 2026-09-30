SELECT o.id, o.store_id FROM orders o WHERE NOT EXISTS (SELECT 1 FROM shipments s WHERE s.order_id = o.id AND s.cost BETWEEN 48 AND 93);
