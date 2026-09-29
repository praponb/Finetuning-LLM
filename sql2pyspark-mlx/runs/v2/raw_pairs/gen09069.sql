SELECT id, sqft, store_name, region FROM stores WHERE sqft >= (SELECT MAX(sqft) FROM stores) ORDER BY store_name DESC, sqft DESC;
