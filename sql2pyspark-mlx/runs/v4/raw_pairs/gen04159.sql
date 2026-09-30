SELECT employees.id, employees.level
FROM employees
WHERE employees.dept_id IN (SELECT id FROM departments WHERE budget >= 1244000)
