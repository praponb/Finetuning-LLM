SELECT hire_date FROM employees WHERE hire_date < '2023-11-01' UNION SELECT hire_date FROM employees WHERE is_remote ORDER BY hire_date DESC;
