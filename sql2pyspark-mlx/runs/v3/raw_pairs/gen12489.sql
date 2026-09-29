SELECT e.id, e.level AS employee_level, e.email AS employee_email, m.salary AS manager_salary FROM employees e LEFT JOIN employees m ON e.manager_id = m.id WHERE m.level = 'L3' AND m.level IS NULL;
