SELECT city, SUM(CASE WHEN region = 'North' THEN sqft ELSE 0 END) AS north_sqft FROM stores WHERE city IN ('New York', 'Singapore', 'Chiang Mai') AND store_name LIKE '%Central%' GROUP BY 1;
