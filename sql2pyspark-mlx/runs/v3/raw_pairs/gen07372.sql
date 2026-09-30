SELECT city, ROUND(AVG(sqft), 2) AS avg_sqft
FROM stores
GROUP BY 1
ORDER BY city;
