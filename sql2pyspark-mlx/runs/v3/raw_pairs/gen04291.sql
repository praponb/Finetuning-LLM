SELECT id, region, sqft, MAX(sqft) OVER (PARTITION BY region) AS region_max_sqft, MIN(sqft) OVER (PARTITION BY region) AS region_min_sqft FROM stores;
