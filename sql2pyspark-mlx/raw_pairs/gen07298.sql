SELECT e.id, e.dept_id, e.salary FROM employees e WHERE e.salary = (SELECT MIN(e2.salary) FROM employees e2 WHERE e2.dept_id = e.dept_id) ORDER BY e.salary;
