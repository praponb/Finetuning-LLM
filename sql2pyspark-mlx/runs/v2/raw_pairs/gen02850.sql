SELECT id, salary, is_remote FROM employees WHERE salary > (SELECT AVG(salary) FROM employees)
