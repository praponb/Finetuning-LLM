SELECT id, salary
FROM employees
WHERE salary >= (SELECT MAX(salary) FROM employees WHERE dept_id IN (3, 22, 36))
ORDER BY id DESC;
