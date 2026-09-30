SELECT e.id, e.dept_id, e.hire_date
FROM employees e
WHERE e.dept_id NOT IN (SELECT id FROM departments WHERE budget < 3659000)
ORDER BY e.hire_date
