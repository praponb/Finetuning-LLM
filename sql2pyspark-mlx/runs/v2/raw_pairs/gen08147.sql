SELECT country, ROUND((100 * SUM(CASE WHEN city = 'Berlin' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_berlin
FROM customers
GROUP BY country
HAVING ROUND((100 * SUM(CASE WHEN city = 'Berlin' THEN 1 ELSE 0 END)) / COUNT(*), 1) > 21;
