SELECT AVG(sqft) AS avg_sqft, SUM(CASE WHEN city = 'Tokyo' THEN 1 ELSE 0 END) AS tokyo_count, COUNT(sqft) AS sqft_count
FROM stores
ORDER BY tokyo_count
