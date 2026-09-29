SELECT oi.order_id, p.price, o.store_id, o.discount FROM order_items oi JOIN products p ON oi.product_id = p.id JOIN orders o ON oi.order_id = o.id WHERE p.category = 'Grocery' AND oi.quantity < 7
