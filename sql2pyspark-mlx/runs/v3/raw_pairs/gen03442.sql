SELECT e.id, e.is_remote FROM employees e WHERE e.dept_id IN (SELECT id FROM departments WHERE location IN ('Tokyo', 'Berlin', 'New York'))
