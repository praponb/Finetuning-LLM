SELECT e.id, e.level FROM employees e WHERE e.hire_date >= '2024-06-01' AND e.dept_id NOT IN (SELECT id FROM departments WHERE location IN ('Tokyo', 'Singapore', 'Bangkok'));
