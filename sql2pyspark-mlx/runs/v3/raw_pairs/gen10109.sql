SELECT o.id, o.discount, o.status, o.customer_id FROM orders o WHERE EXISTS (SELECT 1 FROM shipments s WHERE s.order_id = o.id AND s.delivered_date > '2025-06-01')
