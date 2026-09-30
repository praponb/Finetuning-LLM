SELECT oi.id, oi.order_id, oi.unit_price, o.status FROM order_items oi JOIN orders o ON oi.order_id = o.id WHERE oi.product_id IS NULL
