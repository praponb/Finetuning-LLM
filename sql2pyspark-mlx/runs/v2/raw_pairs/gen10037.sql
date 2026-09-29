SELECT d.id, d.location, d.dept_name
FROM departments d
WHERE d.location NOT IN ('London', 'Tokyo') AND NOT EXISTS (SELECT 1 FROM employees e WHERE e.dept_id = d.id AND e.hire_date < '2023-07-28')
ORDER BY d.dept_name
