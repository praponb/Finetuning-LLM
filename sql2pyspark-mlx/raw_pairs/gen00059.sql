SELECT oi.unit_price, oi.quantity, p.brand, p.cost, o.order_date FROM order_items oi INNER JOIN products p ON p.id = oi.product_id INNER JOIN orders o ON oi.order_id = o.id ORDER BY o.order_date
