SELECT e.id, e.level AS employee_level, m.hire_date AS manager_hire_date FROM employees e LEFT OUTER JOIN employees m ON e.manager_id = m.id ORDER BY manager_hire_date ASC;
