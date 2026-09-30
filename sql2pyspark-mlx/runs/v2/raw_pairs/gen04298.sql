SELECT DISTINCT dept_name
FROM departments
WHERE dept_name IN ('Finance', 'Sales', 'Marketing')
ORDER BY dept_name
LIMIT 5;
