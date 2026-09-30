SELECT oi.order_id, p.brand, o.amount FROM order_items oi FULL OUTER JOIN products p ON p.id = oi.product_id INNER JOIN orders o ON oi.order_id = o.id
