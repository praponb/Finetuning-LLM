SELECT e.id, e.manager_id AS employee_manager_id, m.email AS manager_email, m.is_remote AS manager_is_remote, m.salary AS manager_salary FROM employees e LEFT JOIN employees m ON e.manager_id = m.id;
