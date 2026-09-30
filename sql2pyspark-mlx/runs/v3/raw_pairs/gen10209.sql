SELECT city
FROM stores
WHERE store_name IS NOT NULL
UNION ALL
SELECT city
FROM stores
WHERE city IN ('Singapore', 'Chiang Mai', 'New York')
