SELECT o.id, o.amount, o.channel FROM orders o WHERE NOT EXISTS (SELECT 1 FROM shipments s WHERE s.order_id = o.id) ORDER BY amount DESC, o.id DESC;
