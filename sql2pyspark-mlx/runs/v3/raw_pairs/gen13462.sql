SELECT e.id, e.salary, d.budget, d.location FROM employees e LEFT JOIN departments d ON d.id = e.dept_id WHERE d.dept_name IN ('Engineering', 'Legal', 'HR') AND d.budget BETWEEN 1715000 AND 3713000
