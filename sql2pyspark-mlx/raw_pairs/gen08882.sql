SELECT location
FROM departments
WHERE budget BETWEEN 2052000 AND 2287000
UNION
SELECT location
FROM departments
WHERE location IN ('Singapore', 'Tokyo', 'New York')
ORDER BY location
