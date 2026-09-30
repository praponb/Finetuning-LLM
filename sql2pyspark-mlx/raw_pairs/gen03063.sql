SELECT country
FROM customers
WHERE name LIKE '%Chaiyap%'
UNION ALL
SELECT country
FROM sessions
WHERE started_at >= '2024-08-01'
ORDER BY country DESC;
