SELECT s.region, COUNT(DISTINCT ss.revenue) AS unique_stores
FROM store_sales ss
INNER JOIN products p ON ss.product_id = p.id
LEFT JOIN stores s ON s.id = ss.store_id
GROUP BY s.region
