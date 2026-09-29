SELECT id, hire_date FROM employees WHERE hire_date = (SELECT MIN(hire_date) FROM employees) ORDER BY id
