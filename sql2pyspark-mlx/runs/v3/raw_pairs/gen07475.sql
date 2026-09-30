SELECT p.id, p.category, COUNT(oi.id) AS order_item_count FROM products p LEFT JOIN order_items oi ON p.id = oi.product_id WHERE oi.quantity != 3 GROUP BY p.id, p.category ORDER BY p.id DESC;
