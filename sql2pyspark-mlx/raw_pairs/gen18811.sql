SELECT *, COALESCE(sqft, 0) AS sqft_filled, CONCAT(region, ' ', store_name) AS region_store_name
FROM stores
ORDER BY sqft DESC;
