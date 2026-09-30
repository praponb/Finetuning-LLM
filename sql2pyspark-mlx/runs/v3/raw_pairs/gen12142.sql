SELECT s.region, SUM(ss.revenue) AS total_revenue
FROM store_sales ss
INNER JOIN stores s ON s.id = ss.store_id
GROUP BY s.region
HAVING SUM(ss.revenue) > 10000
ORDER BY total_revenue DESC, s.region;
