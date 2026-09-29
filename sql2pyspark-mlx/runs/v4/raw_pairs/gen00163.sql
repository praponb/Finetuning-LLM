SELECT e.id, e.dept_id, e.full_name, e.manager_id, d.dept_name FROM employees e LEFT JOIN departments d ON d.id = e.dept_id WHERE e.hire_date < '2024-01-27';
