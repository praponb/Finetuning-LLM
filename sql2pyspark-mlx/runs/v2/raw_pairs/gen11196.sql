SELECT e.id, e.email AS employee_email, m.salary AS manager_salary FROM employees e INNER JOIN employees m ON e.manager_id = m.id;
