SELECT id, store_name, sqft, region FROM stores WHERE store_name LIKE 'Do%' AND region IS NOT NULL ORDER BY 2 DESC, id LIMIT 10;
