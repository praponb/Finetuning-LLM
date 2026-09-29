SELECT oi.id, oi.order_id, oi.product_id, o.store_id, o.customer_id, o.amount FROM order_items oi INNER JOIN orders o ON oi.order_id = o.id WHERE o.channel != 'web'
