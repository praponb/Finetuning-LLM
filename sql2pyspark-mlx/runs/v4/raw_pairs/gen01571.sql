SELECT city, SUM(sqft) AS total_sqft, SUM(SUM(sqft)) OVER (ORDER BY city) AS cumulative_sqft FROM stores WHERE sqft <= 11641 GROUP BY city ORDER BY total_sqft
