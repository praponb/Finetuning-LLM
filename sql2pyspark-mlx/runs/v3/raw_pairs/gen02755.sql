SELECT e.id, e.salary AS employee_salary, m.is_remote AS manager_is_remote, m.full_name AS manager_full_name, m.dept_id AS manager_dept_id FROM employees e JOIN employees m ON e.manager_id = m.id;
