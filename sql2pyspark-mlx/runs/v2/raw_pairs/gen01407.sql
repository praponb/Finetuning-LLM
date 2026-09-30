SELECT p.supplier_id, p.stock_qty, p.launch_date, oi.order_id, oi.product_id, oi.quantity FROM products p LEFT OUTER JOIN order_items oi ON oi.product_id = p.id ORDER BY p.stock_qty DESC
