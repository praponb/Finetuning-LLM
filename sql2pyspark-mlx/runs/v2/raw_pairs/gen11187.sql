SELECT id, sqft, store_name FROM stores WHERE sqft = (SELECT MIN(sqft) FROM stores WHERE MONTH(opened_date) IN (5, 7, 11)) ORDER BY sqft DESC
