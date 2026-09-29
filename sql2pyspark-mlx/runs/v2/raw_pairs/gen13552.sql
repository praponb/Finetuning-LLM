SELECT id, region
FROM stores
WHERE sqft <= 15439
UNION ALL
SELECT id, region
FROM stores
WHERE sqft < 7764
