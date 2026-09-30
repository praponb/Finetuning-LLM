SELECT e.full_name, d.dept_name FROM employees e JOIN departments d ON e.dept_id = d.id WHERE e.dept_id IN (9, 15, 23, 26);
