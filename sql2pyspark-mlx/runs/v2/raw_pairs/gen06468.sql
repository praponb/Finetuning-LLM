SELECT d.location, d.dept_name, COUNT(DISTINCT e.salary) AS unique_managers FROM employees e LEFT JOIN departments d ON e.dept_id = d.id GROUP BY d.location, d.dept_name;
