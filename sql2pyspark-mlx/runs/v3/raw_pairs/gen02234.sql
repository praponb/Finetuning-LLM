SELECT city, SUM(CASE WHEN city = 'Bangkok' THEN 1 ELSE 0 END) AS bangkok_count, SUM(CASE WHEN city = 'Chiang Mai' THEN sqft ELSE 0 END) AS chiang_mai_sqft
FROM stores
WHERE city = 'Berlin'
GROUP BY city
ORDER BY bangkok_count DESC, chiang_mai_sqft DESC
