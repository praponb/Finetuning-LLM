SELECT e.id, e.salary, e.email FROM employees e WHERE e.dept_id IN (SELECT id FROM departments WHERE budget BETWEEN 2217000 AND 2885000) ORDER BY email, e.salary DESC, id LIMIT 50;
