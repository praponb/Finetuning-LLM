SELECT city, SUM(sqft) total_sqft, DENSE_RANK() OVER (ORDER BY SUM(sqft) DESC) AS rnk
FROM stores
GROUP BY city;
