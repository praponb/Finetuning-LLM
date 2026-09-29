SELECT d.id, COUNT(e.id) AS employee_count FROM departments d LEFT JOIN employees e ON e.dept_id = d.id GROUP BY d.id HAVING COUNT(e.id) > 4 ORDER BY employee_count DESC, 1 LIMIT 20;
