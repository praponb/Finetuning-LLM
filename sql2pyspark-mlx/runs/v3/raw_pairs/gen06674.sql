SELECT o.id, o.customer_id, o.shipping_fee FROM orders o WHERE EXISTS (SELECT 1 FROM order_items oi WHERE oi.order_id = o.id AND oi.unit_price < 310)
