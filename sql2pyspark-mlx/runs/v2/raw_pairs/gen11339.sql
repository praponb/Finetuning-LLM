SELECT d.id, d.budget
FROM departments d
WHERE d.dept_name NOT IN ('Marketing', 'Engineering', 'Sales') AND NOT EXISTS (SELECT 1 FROM employees e WHERE e.dept_id = d.id AND e.dept_id IN (4, 17, 20))
ORDER BY id DESC
