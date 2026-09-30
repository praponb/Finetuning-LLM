SELECT p.brand, ROUND(AVG(ss.revenue), 2) AS avg_revenue, SUM(ss.units) AS total_units
FROM store_sales ss
JOIN products p ON ss.product_id = p.id
WHERE ss.store_id IN (1, 3, 4, 6)
GROUP BY p.brand
ORDER BY 1 DESC;
