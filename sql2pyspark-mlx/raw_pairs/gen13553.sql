SELECT o.id, o.status, o.customer_id, o.shipping_fee FROM orders o WHERE o.status = 'PENDING' AND o.id IN (SELECT order_id FROM order_items WHERE unit_price > 120) ORDER BY status;
