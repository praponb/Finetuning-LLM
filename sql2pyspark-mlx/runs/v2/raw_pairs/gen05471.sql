SELECT region, COUNT(CASE WHEN region = 'North' THEN 1 END) north_count, COUNT(*) store_count
FROM stores
WHERE region = 'North'
GROUP BY region
ORDER BY region
