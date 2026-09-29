SELECT p.brand, oi.order_id, CONCAT_WS(',', p.product_name, p.category) AS product_name_category_key FROM products p LEFT JOIN order_items oi ON oi.product_id = p.id WHERE p.supplier_id IS NOT NULL
