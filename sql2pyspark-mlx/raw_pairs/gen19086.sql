SELECT p.category, AVG(ss.revenue) AS avg_revenue
FROM store_sales ss
INNER JOIN products p ON p.id = ss.product_id
INNER JOIN stores s ON ss.store_id = s.id
GROUP BY p.category
HAVING AVG(ss.revenue) > 3100
