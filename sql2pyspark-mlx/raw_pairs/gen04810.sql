SELECT
    p.launch_date,
    p.category,
    ss.sale_date,
    ss.store_id
FROM products p
LEFT JOIN store_sales ss ON p.id = ss.product_id
WHERE p.product_name LIKE 'C%';
