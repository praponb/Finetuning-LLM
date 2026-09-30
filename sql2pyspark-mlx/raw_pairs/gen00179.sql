SELECT region, city, COUNT(DISTINCT sqft) AS unique_regions, SUM(CASE WHEN city = 'London' THEN sqft ELSE 0 END) AS london_sqft FROM stores WHERE city != 'Singapore' GROUP BY region, city;
