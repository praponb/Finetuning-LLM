SELECT id, sqft, ROUND(sqft, 1) AS sqft_rounded, CONCAT(region, '/', store_name) AS region_store_name FROM stores
