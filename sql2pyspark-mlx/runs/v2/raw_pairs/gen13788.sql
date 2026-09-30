SELECT SUM(CASE WHEN region = 'South' THEN 1 ELSE 0 END) south_count, MAX(sqft) max_sqft FROM stores
