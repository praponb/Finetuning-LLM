SELECT e.id, e.is_remote AS employee_is_remote, m.is_remote AS manager_is_remote FROM employees e LEFT JOIN employees m ON e.manager_id = m.id ORDER BY e.id DESC LIMIT 3;
