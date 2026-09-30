SELECT city, SUM(sqft) AS total_sqft, COUNT(*) AS cnt, RANK() OVER (ORDER BY SUM(sqft) DESC) AS rnk FROM stores GROUP BY city ORDER BY total_sqft DESC
