SELECT
    p.product_name,
    p.price,
    p.category,
    ss.revenue,
    ss.store_id
FROM products p
LEFT JOIN store_sales ss ON p.id = ss.product_id
WHERE ss.units < 34
ORDER BY ss.store_id DESC;
