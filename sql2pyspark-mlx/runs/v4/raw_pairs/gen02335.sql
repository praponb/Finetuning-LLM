SELECT city, ROUND(STDDEV(sqft), 2) AS stddev_sqft
FROM stores
WHERE city = 'Chiang Mai' AND LOWER(store_name) LIKE '%irp%'
GROUP BY city
HAVING ROUND(STDDEV(sqft), 2) >= 70
ORDER BY city DESC, stddev_sqft
LIMIT 3;
