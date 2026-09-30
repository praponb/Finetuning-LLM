SELECT city, SUM(sqft) AS total_sqft, DENSE_RANK() OVER (ORDER BY SUM(sqft) DESC) AS sqft_rank FROM stores GROUP BY city;
