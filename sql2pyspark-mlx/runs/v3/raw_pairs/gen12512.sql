SELECT country, COUNT(DISTINCT rating) AS unique_countrys, COUNT(CASE WHEN city = 'Bangkok' THEN 1 END) AS bangkok_count
FROM suppliers
GROUP BY country
ORDER BY country;
