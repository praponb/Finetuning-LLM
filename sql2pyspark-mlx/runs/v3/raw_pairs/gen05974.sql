SELECT dept_name, location
FROM departments
WHERE budget >= 2350000
UNION
SELECT dept_name, location
FROM departments
WHERE location IN ('London', 'Singapore', 'Berlin')
ORDER BY location
