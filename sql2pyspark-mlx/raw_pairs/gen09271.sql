SELECT p.id, p.category, p.brand, p.product_name FROM products p WHERE p.id IN (SELECT product_id FROM store_sales WHERE store_id IN (1, 3, 4, 6)) ORDER BY p.id DESC;
