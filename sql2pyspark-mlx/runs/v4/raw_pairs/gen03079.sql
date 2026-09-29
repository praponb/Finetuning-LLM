SELECT p.brand, ROUND(AVG(ss.units), 2) AS avg_units FROM store_sales ss INNER JOIN products p ON ss.product_id = p.id INNER JOIN stores s ON ss.store_id = s.id GROUP BY p.brand ORDER BY avg_units
