SELECT dept_name, location
FROM departments
WHERE dept_name NOT IN ('Finance', 'Sales', 'Legal')
UNION ALL
SELECT dept_name, location
FROM departments
WHERE location NOT IN ('New York', 'London', 'Tokyo')
ORDER BY location;
