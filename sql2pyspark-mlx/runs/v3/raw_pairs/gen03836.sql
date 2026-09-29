SELECT
    p.id,
    p.brand,
    COUNT(ss.id) AS store_sale_count,
    COUNT(ss.revenue) AS revenue_count
FROM products p
LEFT OUTER JOIN store_sales ss ON ss.product_id = p.id
WHERE p.supplier_id IS NOT NULL
GROUP BY p.id, p.brand
ORDER BY revenue_count DESC;
