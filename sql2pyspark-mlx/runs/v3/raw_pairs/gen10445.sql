SELECT city, SUM(CASE WHEN city = 'Bangkok' THEN sqft ELSE 0 END) AS bangkok_sqft
FROM stores
WHERE region != 'West'
GROUP BY city
HAVING SUM(CASE WHEN city = 'Bangkok' THEN sqft ELSE 0 END) < 80
ORDER BY bangkok_sqft;
