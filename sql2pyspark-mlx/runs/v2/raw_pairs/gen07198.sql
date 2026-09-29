SELECT e.id, e.level, e.hire_date
FROM employees e
WHERE e.salary BETWEEN 104000 AND 144000 AND e.dept_id NOT IN (SELECT id FROM departments WHERE location = 'Singapore');
