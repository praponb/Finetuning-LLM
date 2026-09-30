SELECT p.id, COUNT(oi.id) AS order_item_count FROM products p LEFT OUTER JOIN order_items oi ON oi.product_id = p.id GROUP BY p.id;
