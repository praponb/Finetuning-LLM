SELECT region, COUNT(sqft) AS sqft_count, COUNT(DISTINCT sqft) AS unique_regions FROM stores WHERE city = 'New York' GROUP BY region HAVING COUNT(DISTINCT sqft) < 4
