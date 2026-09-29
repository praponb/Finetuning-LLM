SELECT region, SUM(CASE WHEN city = 'Tokyo' THEN 1 ELSE 0 END) AS tokyo_count
FROM stores
WHERE region != 'East'
GROUP BY region;
