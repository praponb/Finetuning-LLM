SELECT oi.id, oi.product_id, oi.unit_price FROM order_items oi WHERE oi.order_id IN (SELECT id FROM orders WHERE order_date > '2024-07-01')
