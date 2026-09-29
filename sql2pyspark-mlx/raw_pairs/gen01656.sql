SELECT id, salary, email
FROM employees
WHERE salary <= (SELECT MIN(salary) FROM employees)
