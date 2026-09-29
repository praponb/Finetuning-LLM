SELECT e.id, e.salary, e.email, d.dept_name, d.location, d.budget FROM employees e INNER JOIN departments d ON e.dept_id = d.id WHERE e.manager_id = 28 ORDER BY id DESC, salary;
