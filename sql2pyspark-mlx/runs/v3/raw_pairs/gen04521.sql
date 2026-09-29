SELECT city, store_name
FROM stores
WHERE sqft <= 6540
UNION ALL
SELECT city, store_name
FROM stores
WHERE city = 'Chiang Mai'
ORDER BY city DESC;
