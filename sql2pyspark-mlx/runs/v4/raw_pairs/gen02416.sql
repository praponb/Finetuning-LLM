SELECT e.id, e.email
FROM employees e
WHERE e.dept_id IN (11, 20, 33) AND e.dept_id IN (SELECT id FROM departments WHERE location IN ('Bangkok', 'New York'));
