SELECT oi.id, oi.order_id, oi.unit_price, oi.product_id, o.customer_id FROM order_items oi LEFT JOIN orders o ON o.id = oi.order_id ORDER BY order_id DESC, o.customer_id DESC, id LIMIT 10
