SELECT city, MAX(sqft) max_sqft, ROUND(STDDEV(sqft), 2) stddev_sqft
FROM stores
WHERE store_name LIKE '%Riverside%'
GROUP BY city
ORDER BY city DESC
LIMIT 10
