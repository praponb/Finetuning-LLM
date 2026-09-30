SELECT *, SUBSTRING(store_name, 1, 4) AS store_name_prefix, CEIL(sqft) AS sqft_whole FROM stores WHERE (sqft = 4513 AND sqft BETWEEN 7277 AND 8426) OR city = 'Tokyo' ORDER BY store_name_prefix;
