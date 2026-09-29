SELECT MAX(sqft) AS max_sqft, MAX(sqft) - MIN(sqft) AS sqft_range, SUM(sqft) AS total_sqft FROM stores
