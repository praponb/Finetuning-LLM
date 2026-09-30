SELECT s.country, MAX(ss.units) AS max_units FROM store_sales ss LEFT JOIN products p ON p.id = ss.product_id INNER JOIN suppliers s ON p.supplier_id = s.id GROUP BY s.country;
