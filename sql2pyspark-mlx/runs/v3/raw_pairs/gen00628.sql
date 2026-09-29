SELECT id, location, dept_name
FROM departments
WHERE budget <= 1999000
UNION
SELECT id, location, dept_name
FROM departments
WHERE location <> 'New York'
ORDER BY location;
