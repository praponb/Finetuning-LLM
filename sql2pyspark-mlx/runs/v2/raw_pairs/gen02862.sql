SELECT city, ROUND(STDDEV(sqft), 2) AS stddev_sqft
FROM stores
GROUP BY city
ORDER BY stddev_sqft ASC, city DESC
LIMIT 3
