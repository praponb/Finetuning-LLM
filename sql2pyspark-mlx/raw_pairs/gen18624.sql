SELECT id, city, opened_date
FROM stores
WHERE LENGTH(store_name) >= 9
UNION ALL
SELECT id, city, opened_date
FROM stores
WHERE sqft <= 7670
ORDER BY opened_date
