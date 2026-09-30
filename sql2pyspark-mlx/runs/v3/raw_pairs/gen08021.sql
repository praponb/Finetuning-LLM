SELECT oi.id, oi.product_id, oi.unit_price, o.channel, p.brand FROM order_items oi LEFT OUTER JOIN orders o ON o.id = oi.order_id JOIN products p ON oi.product_id = p.id WHERE o.customer_id IS NULL
