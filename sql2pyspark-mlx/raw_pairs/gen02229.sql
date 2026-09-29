SELECT id, sqft, opened_date, store_name FROM stores WHERE sqft < (SELECT MIN(sqft) FROM stores);
