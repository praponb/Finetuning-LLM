SELECT oi.id, oi.quantity, oi.order_id, p.stock_qty, p.brand, p.launch_date FROM order_items oi JOIN products p ON oi.product_id = p.id WHERE oi.quantity BETWEEN 4 AND 8
