SELECT id, salary
FROM employees
WHERE salary >= (SELECT MAX(salary) FROM employees)
ORDER BY salary DESC, id DESC;
