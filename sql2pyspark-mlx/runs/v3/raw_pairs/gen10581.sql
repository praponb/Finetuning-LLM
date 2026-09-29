SELECT s.id, s.order_id, o.customer_id, o.store_id FROM shipments s JOIN orders o ON s.order_id = o.id ORDER BY store_id, id LIMIT 50;
