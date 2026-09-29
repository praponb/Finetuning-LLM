SELECT city, COUNT(*) AS row_count, COUNT(sqft) AS sqft_count FROM stores GROUP BY 1 HAVING COUNT(sqft) >= 1
