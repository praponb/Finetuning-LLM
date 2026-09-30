SELECT *, LENGTH(dept_name) AS dept_name_length
FROM departments
WHERE dept_name IN ('Sales', 'HR');
