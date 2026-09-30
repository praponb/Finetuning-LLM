SELECT country, SUM(CASE WHEN city = 'Bangkok' THEN 1 ELSE 0 END) AS bangkok_count
FROM suppliers
GROUP BY country
HAVING SUM(CASE WHEN city = 'Bangkok' THEN 1 ELSE 0 END) > 41
ORDER BY country;
