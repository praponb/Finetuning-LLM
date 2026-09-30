SELECT a.id, a.dept_id, a.salary, a.is_remote FROM employees a WHERE a.salary < (SELECT AVG(b.salary) FROM employees b WHERE b.dept_id = a.dept_id)
