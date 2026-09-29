SELECT d.location, MAX(e.salary) AS max_salary FROM employees e INNER JOIN departments d ON e.dept_id = d.id WHERE d.location != 'Bangkok' GROUP BY d.location;
