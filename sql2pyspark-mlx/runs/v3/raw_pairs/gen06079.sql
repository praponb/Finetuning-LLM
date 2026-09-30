SELECT e.full_name AS employee_full_name, e.dept_id AS employee_dept_id, m.is_remote AS manager_is_remote FROM employees e LEFT JOIN employees m ON e.manager_id = m.id WHERE e.full_name IS NOT NULL;
