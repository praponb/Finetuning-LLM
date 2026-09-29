SELECT
    p.id,
    p.brand,
    COUNT(ss.id) AS store_sale_count,
    AVG(ss.units) AS avg_units,
    MIN(ss.sale_date) AS min_sale_date
FROM products p
LEFT JOIN store_sales ss ON ss.product_id = p.id
WHERE ss.store_id IN (1, 3, 4, 5)
GROUP BY p.id, p.brand;
