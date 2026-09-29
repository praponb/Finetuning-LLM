SELECT e.level, d.location, d.budget
FROM employees e
RIGHT JOIN departments d ON e.dept_id = d.id;
