SELECT
    p.id,
    p.brand,
    COUNT(ss.id) AS store_sale_count,
    MAX(ss.sale_date) AS last_sale_date
FROM products p
LEFT JOIN store_sales ss ON ss.product_id = p.id
WHERE p.price IS NULL
GROUP BY p.id, p.brand
ORDER BY last_sale_date, p.id;
