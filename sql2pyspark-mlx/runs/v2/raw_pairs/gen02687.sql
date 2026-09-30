SELECT region, city, ROUND((100 * SUM(CASE WHEN region = 'Central' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_central
FROM stores
GROUP BY region, city
ORDER BY 2 DESC
