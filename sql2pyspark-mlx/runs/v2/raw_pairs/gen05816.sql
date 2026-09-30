SELECT p.category, p.product_name, oi.order_id FROM products p LEFT OUTER JOIN order_items oi ON p.id = oi.product_id ORDER BY product_name, category;
