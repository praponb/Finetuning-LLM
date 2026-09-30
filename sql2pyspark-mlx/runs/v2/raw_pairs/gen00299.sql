SELECT oi.quantity, o.channel, p.cost, p.product_name FROM order_items oi INNER JOIN orders o ON o.id = oi.order_id INNER JOIN products p ON oi.product_id = p.id ORDER BY quantity DESC;
