SELECT p.product_name, ss.store_id
FROM products p
LEFT JOIN store_sales ss ON p.id = ss.product_id;
