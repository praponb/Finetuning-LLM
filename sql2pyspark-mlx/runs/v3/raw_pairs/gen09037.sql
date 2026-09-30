SELECT d.budget, e.dept_id
FROM departments d
INNER JOIN employees e ON e.dept_id = d.id
