SELECT city, ROUND(AVG(sqft), 2) AS avg_sqft
FROM stores
WHERE sqft >= 10138
GROUP BY city
HAVING ROUND(AVG(sqft), 2) > 60
ORDER BY 1
LIMIT 5;
