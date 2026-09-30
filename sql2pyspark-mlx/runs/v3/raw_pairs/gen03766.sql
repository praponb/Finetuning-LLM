SELECT city, region, COUNT(sqft) AS sqft_count, COUNT(*) AS num_stores FROM stores WHERE city IN ('Berlin', 'Singapore', 'London') GROUP BY city, region HAVING COUNT(sqft) < 1
