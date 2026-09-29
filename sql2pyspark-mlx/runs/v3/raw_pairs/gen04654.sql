SELECT id, sqft, city, store_name FROM stores WHERE sqft > (SELECT AVG(sqft) FROM stores);
