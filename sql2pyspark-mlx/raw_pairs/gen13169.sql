SELECT e.is_remote AS employee_is_remote, m.hire_date AS manager_hire_date
FROM employees AS e
INNER JOIN employees AS m ON e.manager_id = m.id
ORDER BY manager_hire_date ASC
