SELECT id, salary FROM employees WHERE salary > (SELECT AVG(salary) FROM employees WHERE is_remote = TRUE) ORDER BY id
