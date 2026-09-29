SELECT city, SUM(sqft) AS total_sqft, COUNT(*) AS cnt, SUM(SUM(sqft)) OVER (ORDER BY city) AS cumulative_sqft FROM stores WHERE sqft IS NOT NULL GROUP BY city ORDER BY cnt DESC, cumulative_sqft DESC;
