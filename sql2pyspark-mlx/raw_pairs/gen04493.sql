SELECT p.supplier_id, oi.quantity FROM products p LEFT JOIN order_items oi ON oi.product_id = p.id WHERE p.stock_qty <= 248
