SELECT o.id, o.customer_id, o.store_id, o.shipping_fee FROM orders o WHERE o.id IN (SELECT order_id FROM shipments WHERE order_id IN (10, 15, 29, 37));
