SELECT e.salary, e.dept_id, e.is_remote, d.dept_name, LOWER(e.email) AS email_lower FROM employees e LEFT OUTER JOIN departments d ON e.dept_id = d.id;
