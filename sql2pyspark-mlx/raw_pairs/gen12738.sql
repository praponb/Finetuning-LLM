SELECT id, region, store_name, CONCAT(store_name, ', ', region) AS store_name_region FROM stores WHERE sqft != 10986 OR sqft = 12904;
