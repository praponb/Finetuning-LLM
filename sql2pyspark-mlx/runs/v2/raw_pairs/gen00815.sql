SELECT e.id, e.dept_id, e.is_remote
FROM employees e
WHERE LENGTH(e.email) > 10 AND e.dept_id NOT IN (SELECT id FROM departments WHERE dept_name NOT IN ('Finance', 'Sales'));
