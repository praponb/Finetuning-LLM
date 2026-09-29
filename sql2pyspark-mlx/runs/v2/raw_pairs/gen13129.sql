SELECT dept_name, location
FROM departments
WHERE budget >= 3313000
UNION ALL
SELECT dept_name, location
FROM departments
WHERE dept_name IN ('Legal', 'Finance')
ORDER BY location;
