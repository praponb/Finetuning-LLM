SELECT id, region, sqft, SUM(sqft) OVER (PARTITION BY region) AS region_sum_sqft FROM stores WHERE store_name IS NOT NULL;
