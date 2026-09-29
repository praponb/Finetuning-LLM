SELECT budget, dept_name, LOWER(dept_name) AS dept_name_lower
FROM departments
ORDER BY dept_name_lower;
