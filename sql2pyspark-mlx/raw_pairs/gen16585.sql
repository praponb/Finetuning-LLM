SELECT id, region, sqft, store_name, FIRST_VALUE(store_name) OVER (PARTITION BY region ORDER BY sqft DESC, id) AS first_store_name FROM stores;
