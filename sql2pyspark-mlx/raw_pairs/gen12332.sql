SELECT s.city, s.region, COUNT(*) AS total_count FROM store_sales ss LEFT OUTER JOIN stores s ON ss.store_id = s.id WHERE s.sqft BETWEEN 9411 AND 10462 GROUP BY s.city, s.region ORDER BY total_count;
