SELECT e.id, e.level, e.hire_date
FROM employees e
WHERE e.dept_id NOT IN (SELECT id FROM departments WHERE location = 'Chiang Mai')
