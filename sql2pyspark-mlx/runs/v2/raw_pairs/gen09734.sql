SELECT s.city, s.region, COUNT(*) AS row_count FROM store_sales ss INNER JOIN products p ON ss.product_id = p.id LEFT JOIN stores s ON s.id = ss.store_id GROUP BY s.city, s.region ORDER BY region
