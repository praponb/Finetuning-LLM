SELECT e.id, e.full_name AS employee_full_name, m.email AS manager_email, m.level AS manager_level FROM employees e INNER JOIN employees m ON e.manager_id = m.id;
