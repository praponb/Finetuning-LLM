SELECT o.id, o.discount, o.customer_id, o.order_date FROM orders o WHERE NOT EXISTS (SELECT 1 FROM shipments s WHERE s.order_id = o.id AND s.carrier <> 'UPS') ORDER BY o.customer_id DESC;
