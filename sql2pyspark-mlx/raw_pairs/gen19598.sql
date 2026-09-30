SELECT e.id, e.salary FROM employees e WHERE e.dept_id IN (SELECT id FROM departments WHERE location = 'Tokyo') ORDER BY id DESC
