SELECT employees.id, employees.salary, employees.email
FROM employees
WHERE employees.level = 'L3' AND employees.dept_id IN (SELECT id FROM departments WHERE budget BETWEEN 2965000 AND 3048000)
ORDER BY id DESC
