SELECT city, MAX(sqft) AS max_sqft, MIN(opened_date) AS min_opened_date
FROM stores
GROUP BY city
ORDER BY max_sqft DESC;
