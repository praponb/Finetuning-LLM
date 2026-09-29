SELECT region, SUM(sqft) AS total_sqft, RANK() OVER (ORDER BY SUM(sqft) DESC) AS sqft_rank
FROM stores
GROUP BY region
