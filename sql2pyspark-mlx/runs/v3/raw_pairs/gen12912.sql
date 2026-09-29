SELECT d.budget, e.is_remote
FROM departments d
LEFT JOIN employees e ON d.id = e.dept_id
ORDER BY d.budget DESC;
