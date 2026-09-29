SELECT
    p.category,
    COUNT(ss.id) AS store_sale_count,
    COUNT(DISTINCT ss.units) AS unique_stores,
    SUM(ss.units) AS total_units
FROM products p
LEFT JOIN store_sales ss ON ss.product_id = p.id
WHERE ss.sale_date < '2024-05-01'
GROUP BY p.category
