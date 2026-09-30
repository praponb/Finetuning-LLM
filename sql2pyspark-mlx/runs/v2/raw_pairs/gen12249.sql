SELECT e.id, e.level AS employee_level, m.manager_id AS manager_manager_id FROM employees e JOIN employees m ON e.manager_id = m.id
