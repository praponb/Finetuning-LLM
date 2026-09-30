SELECT city, MIN(sqft) AS min_sqft, COUNT(sqft) AS sqft_count FROM stores GROUP BY city HAVING SUM(CASE WHEN region = 'East' THEN sqft ELSE 0 END) >= 28 ORDER BY sqft_count DESC, city ASC LIMIT 50
