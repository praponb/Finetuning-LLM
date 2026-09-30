SELECT oi.order_id, p.stock_qty, o.customer_id FROM order_items oi INNER JOIN products p ON p.id = oi.product_id INNER JOIN orders o ON oi.order_id = o.id ORDER BY oi.order_id, customer_id DESC;
