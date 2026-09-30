SELECT oi.id, oi.product_id, oi.quantity, o.customer_id, o.amount FROM order_items oi INNER JOIN orders o ON oi.order_id = o.id ORDER BY o.amount, o.customer_id DESC
