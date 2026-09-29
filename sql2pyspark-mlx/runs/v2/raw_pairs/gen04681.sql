SELECT region, SUM(sqft) AS total_sqft, COUNT(*) AS cnt, SUM(SUM(sqft)) OVER (ORDER BY region) AS cumulative_sqft FROM stores WHERE region IS NOT NULL GROUP BY region ORDER BY cumulative_sqft DESC;
