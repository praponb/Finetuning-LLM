SELECT id, salary, hire_date
FROM employees
WHERE salary >= (SELECT AVG(salary) FROM employees)
ORDER BY salary DESC, id DESC;
