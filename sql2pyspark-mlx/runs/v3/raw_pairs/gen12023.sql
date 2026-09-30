SELECT p.stock_qty, oi.quantity, oi.product_id, oi.order_id FROM products p LEFT JOIN order_items oi ON oi.product_id = p.id ORDER BY order_id
