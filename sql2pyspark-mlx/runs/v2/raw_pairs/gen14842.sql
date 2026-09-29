SELECT id, sqft, city, opened_date FROM stores WHERE sqft >= (SELECT MAX(sqft) FROM stores) ORDER BY id DESC LIMIT 5
