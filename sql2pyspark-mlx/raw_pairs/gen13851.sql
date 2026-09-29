SELECT s.city, MAX(ss.revenue) - MIN(ss.revenue) AS revenue_range
FROM store_sales ss
INNER JOIN products p ON ss.product_id = p.id
INNER JOIN stores s ON s.id = ss.store_id
GROUP BY s.city
ORDER BY s.city;
