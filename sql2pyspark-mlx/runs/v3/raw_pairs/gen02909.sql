SELECT oi.product_id, oi.quantity, oi.unit_price, o.discount FROM order_items oi INNER JOIN orders o ON oi.order_id = o.id ORDER BY product_id DESC
