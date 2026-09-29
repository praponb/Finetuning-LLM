SELECT e.id, e.dept_id, e.salary, e.hire_date FROM employees e WHERE e.salary < (SELECT AVG(e2.salary) FROM employees e2 WHERE e2.dept_id = e.dept_id);
