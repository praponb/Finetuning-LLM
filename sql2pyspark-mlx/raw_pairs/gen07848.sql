SELECT id, sqft, opened_date
FROM stores
WHERE sqft >= (SELECT MAX(sqft) FROM stores);
