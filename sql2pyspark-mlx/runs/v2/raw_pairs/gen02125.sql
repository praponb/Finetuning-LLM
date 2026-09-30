SELECT
    s.id,
    COUNT(ss.id) AS store_sale_count,
    MAX(ss.revenue) - MIN(ss.revenue) AS revenue_range,
    COUNT(ss.units) AS units_count
FROM stores s
LEFT JOIN store_sales ss ON s.id = ss.store_id
WHERE ss.units < 15
GROUP BY s.id
ORDER BY id DESC
