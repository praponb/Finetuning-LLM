SELECT oi.id, oi.order_id, oi.quantity, p.product_name, p.launch_date, o.amount FROM order_items oi RIGHT JOIN products p ON oi.product_id = p.id LEFT JOIN orders o ON o.id = oi.order_id
