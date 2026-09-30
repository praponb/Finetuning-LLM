SELECT id, sqft, region, city FROM stores WHERE sqft < (SELECT MIN(sqft) FROM stores WHERE LOWER(TRIM(store_name)) = 'siam flagship');
