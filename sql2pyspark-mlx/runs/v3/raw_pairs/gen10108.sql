SELECT id, sqft, city FROM stores WHERE sqft >= (SELECT AVG(sqft) FROM stores) ORDER BY city, id LIMIT 5;
