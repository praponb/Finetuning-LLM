SELECT oi.id, oi.product_id, oi.quantity, oi.order_id FROM order_items oi WHERE oi.product_id = 22 AND oi.order_id IN (SELECT id FROM orders WHERE order_date BETWEEN '2023-08-17' AND '2024-07-01');
