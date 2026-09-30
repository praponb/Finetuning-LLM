SELECT city, SUM(sqft) AS total_sqft, DENSE_RANK() OVER (ORDER BY SUM(sqft) DESC) AS sqft_rank FROM stores WHERE region = 'North' GROUP BY city ORDER BY city;
