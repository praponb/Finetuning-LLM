SELECT o.id, o.channel FROM orders o WHERE o.customer_id IS NULL AND EXISTS (SELECT 1 FROM shipments s WHERE s.order_id = o.id AND s.cost IS NOT NULL);
