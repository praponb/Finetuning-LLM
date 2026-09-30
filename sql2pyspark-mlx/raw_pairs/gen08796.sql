SELECT e.id, e.dept_id, e.salary, e.level FROM employees e WHERE e.salary = (SELECT MAX(e2.salary) FROM employees e2 WHERE e2.dept_id = e.dept_id);
