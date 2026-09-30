SELECT e.full_name, d.budget, d.dept_name FROM employees e LEFT OUTER JOIN departments d ON d.id = e.dept_id;
