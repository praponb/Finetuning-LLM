SELECT e.full_name AS employee_full_name, m.dept_id AS manager_dept_id, m.salary AS manager_salary FROM employees e LEFT JOIN employees m ON e.manager_id = m.id ORDER BY employee_full_name
