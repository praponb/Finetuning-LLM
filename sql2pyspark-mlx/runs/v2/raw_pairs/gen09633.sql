SELECT
    p.id,
    p.product_name,
    COUNT(ss.id) AS store_sale_count,
    ROUND(STDDEV(ss.units), 2) AS stddev_units,
    MIN(ss.sale_date) AS min_sale_date
FROM products p
JOIN store_sales ss ON ss.product_id = p.id
WHERE ss.units BETWEEN 12 AND 13
GROUP BY p.id, p.product_name
ORDER BY stddev_units ASC
