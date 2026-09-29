SELECT country, COUNT(*) AS total_count, SUM(age) AS total_age
FROM customers
WHERE segment IN ('SMB', 'Corporate')
GROUP BY country
ORDER BY total_count DESC;
