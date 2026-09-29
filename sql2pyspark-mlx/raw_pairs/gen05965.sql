SELECT s.city, COUNT(ss.units) AS units_count, MAX(ss.revenue) - MIN(ss.revenue) AS revenue_range
FROM store_sales ss
INNER JOIN products p ON ss.product_id = p.id
INNER JOIN suppliers s ON p.supplier_id = s.id
WHERE p.cost > 250
GROUP BY s.city;
