SELECT p.category, MAX(ss.units) - MIN(ss.units) AS units_range, ROUND(AVG(ss.units), 2) AS avg_units FROM store_sales ss INNER JOIN products p ON ss.product_id = p.id GROUP BY p.category;
