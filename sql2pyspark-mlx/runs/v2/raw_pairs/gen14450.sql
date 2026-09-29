SELECT city, SUM(sqft) AS total_sqft FROM stores GROUP BY city ORDER BY city DESC;
