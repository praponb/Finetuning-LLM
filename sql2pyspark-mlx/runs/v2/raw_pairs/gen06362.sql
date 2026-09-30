SELECT region, city, ROUND(STDDEV(sqft), 2) AS stddev_sqft
FROM stores
GROUP BY region, city
HAVING ROUND(STDDEV(sqft), 2) < 34
ORDER BY city DESC
