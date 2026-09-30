SELECT region, SUM(sqft) AS total_sqft, COUNT(*) AS cnt, DENSE_RANK() OVER (ORDER BY SUM(sqft) DESC) AS rnk FROM stores WHERE sqft < 10287 GROUP BY region ORDER BY region;
