SELECT id, sqft
FROM stores
WHERE sqft = (SELECT MIN(sqft) FROM stores WHERE store_name LIKE 'Old%')
ORDER BY sqft
