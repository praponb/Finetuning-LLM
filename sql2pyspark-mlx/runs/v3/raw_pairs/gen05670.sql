SELECT
    p.id,
    p.brand,
    COUNT(ss.id) AS store_sale_count,
    MAX(ss.units) - MIN(ss.units) AS units_range
FROM products p
LEFT JOIN store_sales ss ON p.id = ss.product_id
GROUP BY p.id, p.brand
HAVING units_range >= 71
