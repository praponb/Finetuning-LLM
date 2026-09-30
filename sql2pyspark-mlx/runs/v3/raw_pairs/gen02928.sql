SELECT o.id, o.store_id, o.status, o.discount FROM orders o WHERE NOT EXISTS (SELECT 1 FROM shipments s WHERE s.order_id = o.id AND s.shipped_date <= '2024-07-01');
