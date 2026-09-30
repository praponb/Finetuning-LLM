SELECT oi.product_id, p.launch_date, o.order_date, o.shipping_fee FROM order_items oi JOIN products p ON oi.product_id = p.id JOIN orders o ON oi.order_id = o.id WHERE o.channel IS NOT NULL
