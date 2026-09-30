SELECT id, sqft, region, store_name FROM stores WHERE sqft >= (SELECT MAX(sqft) FROM stores) ORDER BY id DESC LIMIT 10;
