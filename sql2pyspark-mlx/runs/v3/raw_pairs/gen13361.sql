SELECT dept_name, location
FROM departments
WHERE budget > 1197000
UNION ALL
SELECT dept_name, location
FROM departments
WHERE budget < 3222000
ORDER BY location;
