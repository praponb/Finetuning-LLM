SELECT city, COUNT(sqft) AS sqft_count, SUM(CASE WHEN city = 'Tokyo' THEN 1 ELSE 0 END) AS tokyo_count FROM stores GROUP BY city ORDER BY sqft_count DESC
