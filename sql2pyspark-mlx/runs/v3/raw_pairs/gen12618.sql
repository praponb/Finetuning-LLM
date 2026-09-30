SELECT city, SUM(sqft) AS total_sqft, COUNT(*) AS cnt, SUM(SUM(sqft)) OVER (ORDER BY city) AS cumulative_sqft FROM stores GROUP BY city ORDER BY city DESC
