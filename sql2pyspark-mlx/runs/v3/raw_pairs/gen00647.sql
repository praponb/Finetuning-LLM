SELECT city, ROUND((100 * SUM(CASE WHEN city = 'Tokyo' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_tokyo, COUNT(DISTINCT age) AS unique_countrys
FROM customers
WHERE (name LIKE 'O%' AND segment IS NOT NULL) OR age IS NOT NULL
GROUP BY city
HAVING unique_countrys < 3;
