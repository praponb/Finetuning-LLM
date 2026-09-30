SELECT id, hire_date FROM employees WHERE hire_date = (SELECT MAX(hire_date) FROM employees WHERE is_remote) ORDER BY hire_date, id DESC;
