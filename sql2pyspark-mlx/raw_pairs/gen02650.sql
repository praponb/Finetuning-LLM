SELECT e.id, e.level AS employee_level, m.email AS manager_email
FROM employees e
LEFT OUTER JOIN employees m ON e.manager_id = m.id
