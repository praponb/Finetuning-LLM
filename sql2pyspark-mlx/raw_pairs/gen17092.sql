SELECT id, region, sqft, store_name FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY region ORDER BY sqft DESC, id) pos FROM stores WHERE region = 'West') t WHERE pos = 1 ORDER BY sqft DESC;
