SELECT id, sqft FROM stores WHERE region = 'North' UNION SELECT id, sqft FROM stores WHERE LENGTH(store_name) > 13 ORDER BY sqft DESC
