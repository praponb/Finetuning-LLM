SELECT e.id, e.hire_date
FROM employees e
WHERE e.dept_id IN (SELECT id FROM departments WHERE location IS NOT NULL)
