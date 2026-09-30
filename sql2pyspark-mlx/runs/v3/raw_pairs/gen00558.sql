SELECT city, ROUND(AVG(sqft), 2) AS avg_sqft FROM stores GROUP BY city ORDER BY avg_sqft DESC;
