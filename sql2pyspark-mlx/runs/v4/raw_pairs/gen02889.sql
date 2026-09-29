SELECT e.is_remote AS employee_is_remote, m.dept_id AS manager_dept_id, m.level AS manager_level FROM employees e INNER JOIN employees m ON e.manager_id = m.id;
