SELECT
    s.id,
    s.region,
    COUNT(ss.id) AS store_sale_count,
    SUM(ss.revenue) AS total_revenue,
    AVG(ss.revenue) AS avg_revenue
FROM stores s
JOIN store_sales ss ON s.id = ss.store_id
GROUP BY s.id, s.region
