SELECT region, COUNT(sqft) AS sqft_count, SUM(CASE WHEN region = 'East' THEN sqft ELSE 0 END) AS east_sqft FROM stores GROUP BY 1
