SELECT e.dept_id, e.email, d.location, d.dept_name FROM employees e JOIN departments d ON d.id = e.dept_id WHERE d.budget BETWEEN 1289000 AND 2585000 ORDER BY d.dept_name
