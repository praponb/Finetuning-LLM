SELECT id, salary FROM employees WHERE salary = (SELECT MIN(salary) FROM employees WHERE salary BETWEEN 63000 AND 115000);
