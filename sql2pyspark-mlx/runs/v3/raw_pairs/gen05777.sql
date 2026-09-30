SELECT id, store_name
FROM stores
WHERE sqft >= 12215
EXCEPT
SELECT id, store_name
FROM stores
WHERE sqft > 12668
