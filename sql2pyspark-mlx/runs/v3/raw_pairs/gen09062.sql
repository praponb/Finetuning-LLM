SELECT p.category, oi.quantity FROM products p LEFT JOIN order_items oi ON oi.product_id = p.id
