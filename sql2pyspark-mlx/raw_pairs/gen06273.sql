SELECT id, salary FROM employees WHERE salary > (SELECT AVG(salary) FROM employees) ORDER BY salary DESC, id LIMIT 20;
