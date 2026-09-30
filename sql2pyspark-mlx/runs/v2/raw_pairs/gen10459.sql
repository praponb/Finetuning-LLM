SELECT d.dept_name, MAX(e.salary) AS max_salary FROM employees e INNER JOIN departments d ON e.dept_id = d.id WHERE e.level <> 'L4' GROUP BY d.dept_name ORDER BY dept_name DESC LIMIT 10;
