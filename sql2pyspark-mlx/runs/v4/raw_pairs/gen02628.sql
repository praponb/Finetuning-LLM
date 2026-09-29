SELECT e.id, e.full_name FROM employees e WHERE e.full_name LIKE 'P%' AND e.dept_id IN (SELECT id FROM departments WHERE dept_name IS NOT NULL) ORDER BY full_name DESC, id DESC;
