SELECT p.category, p.brand, AVG(ss.revenue) AS avg_revenue
FROM store_sales ss
INNER JOIN products p ON ss.product_id = p.id
GROUP BY p.category, p.brand
ORDER BY avg_revenue DESC, p.category, brand
LIMIT 10
