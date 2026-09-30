SELECT oi.id, oi.unit_price, p.product_name, p.brand, o.amount FROM order_items oi LEFT JOIN products p ON p.id = oi.product_id JOIN orders o ON oi.order_id = o.id WHERE p.cost >= 240
