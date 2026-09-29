SELECT id, sqft, region, opened_date FROM stores WHERE sqft >= (SELECT MAX(sqft) FROM stores) ORDER BY opened_date
