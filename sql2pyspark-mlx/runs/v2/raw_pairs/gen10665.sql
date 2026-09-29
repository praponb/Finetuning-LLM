SELECT id, sqft FROM stores WHERE sqft <= (SELECT MIN(sqft) FROM stores)
