SELECT id, salary
FROM employees
WHERE salary >= (SELECT AVG(salary) FROM employees WHERE LENGTH(full_name) < 12)
ORDER BY 2, id
LIMIT 10;
