SELECT a.id, a.dept_id, a.salary, a.manager_id FROM employees a WHERE a.salary > (SELECT AVG(b.salary) FROM employees b WHERE b.dept_id = a.dept_id);
