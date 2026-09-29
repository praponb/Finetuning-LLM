SELECT
    s.region,
    s.city,
    COUNT(ss.revenue) AS revenue_count,
    MIN(ss.revenue) AS min_revenue
FROM store_sales ss
INNER JOIN products p ON ss.product_id = p.id
LEFT JOIN stores s ON s.id = ss.store_id
GROUP BY s.region, s.city
ORDER BY s.city, region
LIMIT 3;
