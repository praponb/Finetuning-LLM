SELECT oi.id, oi.product_id, oi.quantity FROM order_items oi WHERE oi.unit_price BETWEEN 200 AND 380 AND oi.order_id NOT IN (SELECT id FROM orders WHERE discount <= 0.19)
