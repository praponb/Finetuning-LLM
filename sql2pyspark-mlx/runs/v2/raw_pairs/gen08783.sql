SELECT p.category, MAX(ss.units) - MIN(ss.units) AS units_range
FROM store_sales ss
INNER JOIN stores s ON ss.store_id = s.id
RIGHT JOIN products p ON ss.product_id = p.id
GROUP BY p.category
ORDER BY p.category ASC, units_range DESC
LIMIT 20;
