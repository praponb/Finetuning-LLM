SELECT p.price, p.launch_date, oi.product_id FROM products p LEFT JOIN order_items oi ON oi.product_id = p.id;
