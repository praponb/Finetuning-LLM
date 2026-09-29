SELECT region, MIN(sqft) AS min_sqft, MAX(sqft) - MIN(sqft) AS sqft_range, MIN(opened_date) AS first_opened_date FROM stores GROUP BY region
