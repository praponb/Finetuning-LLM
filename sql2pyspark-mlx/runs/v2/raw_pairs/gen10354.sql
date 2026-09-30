SELECT region, city, SUM(sqft) total_sqft, COUNT(*) cnt FROM stores GROUP BY region, city ORDER BY city ASC;
