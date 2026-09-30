SELECT id, region, city, sqft, MIN(sqft) OVER (PARTITION BY region, city) region_min_sqft FROM stores;
