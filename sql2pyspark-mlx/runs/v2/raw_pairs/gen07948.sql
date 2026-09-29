SELECT oi.order_id, p.brand, p.launch_date FROM order_items oi INNER JOIN products p ON p.id = oi.product_id WHERE p.stock_qty < 311 AND oi.order_id IN (12, 21, 26);
