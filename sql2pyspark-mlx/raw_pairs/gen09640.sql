SELECT YEAR(opened_date) AS opened_year, city, COUNT(*) AS num_stores
FROM stores
GROUP BY 1, 2
HAVING SUM(CASE WHEN city = 'Tokyo' THEN 1 ELSE 0 END) >= 48
ORDER BY num_stores DESC
