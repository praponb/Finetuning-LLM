SELECT ROUND((100 * SUM(CASE WHEN city = 'Tokyo' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_tokyo
FROM stores
