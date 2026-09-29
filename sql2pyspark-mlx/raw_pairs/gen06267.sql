SELECT e.is_remote AS employee_is_remote, m.email AS manager_email, m.salary AS manager_salary FROM employees e LEFT OUTER JOIN employees m ON e.manager_id = m.id WHERE e.is_remote;
