SELECT id, salary, full_name
FROM employees
WHERE salary > (SELECT MAX(salary) FROM employees)
ORDER BY salary
