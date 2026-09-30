SELECT d.dept_name, MAX(e.salary) - MIN(e.salary) AS salary_range FROM employees e LEFT OUTER JOIN departments d ON e.dept_id = d.id GROUP BY d.dept_name;
