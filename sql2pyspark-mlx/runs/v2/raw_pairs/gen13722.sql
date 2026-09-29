SELECT p.id, p.category, COUNT(oi.id) order_item_count FROM products p LEFT JOIN order_items oi ON oi.product_id = p.id WHERE p.product_name LIKE 'Ba%' GROUP BY p.id, p.category ORDER BY 1 DESC;
