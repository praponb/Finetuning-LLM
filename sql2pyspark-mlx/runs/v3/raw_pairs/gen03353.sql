SELECT p.category, AVG(ss.units) avg_units FROM store_sales ss FULL OUTER JOIN products p ON ss.product_id = p.id JOIN stores s ON ss.store_id = s.id GROUP BY p.category
