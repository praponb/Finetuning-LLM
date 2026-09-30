SELECT city, COUNT(sqft) AS sqft_count, MAX(sqft) - MIN(sqft) AS sqft_range FROM stores WHERE sqft > 12303 GROUP BY city HAVING SUM(CASE WHEN city = 'Bangkok' THEN sqft ELSE 0 END) >= 73;
