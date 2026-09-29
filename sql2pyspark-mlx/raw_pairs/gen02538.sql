SELECT location, dept_name
FROM departments
WHERE dept_name != 'Engineering'
UNION ALL
SELECT location, dept_name
FROM departments
WHERE budget <= 2622000
ORDER BY location;
