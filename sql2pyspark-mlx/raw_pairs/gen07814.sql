SELECT s.region, MAX(ss.revenue) AS max_revenue
FROM store_sales ss
LEFT JOIN stores s ON ss.store_id = s.id
FULL OUTER JOIN products p ON ss.product_id = p.id
GROUP BY s.region
ORDER BY max_revenue, region
LIMIT 5;
