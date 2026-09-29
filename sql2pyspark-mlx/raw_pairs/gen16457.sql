SELECT e.id, e.manager_id, e.is_remote
FROM employees e
WHERE e.manager_id IS NULL AND e.dept_id IN (SELECT id FROM departments WHERE location = 'Tokyo');
