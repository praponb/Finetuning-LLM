SELECT s.city, ROUND(AVG(ss.revenue), 2) AS avg_revenue, COUNT(ss.units) AS units_count
FROM store_sales ss
INNER JOIN stores s ON ss.store_id = s.id
LEFT OUTER JOIN products p ON p.id = ss.product_id
GROUP BY s.city
ORDER BY units_count, avg_revenue DESC, city
LIMIT 50;
