SELECT o.id, o.shipping_fee, o.customer_id, o.channel FROM orders o WHERE o.discount IS NOT NULL AND NOT EXISTS (SELECT 1 FROM shipments s WHERE s.order_id = o.id);
