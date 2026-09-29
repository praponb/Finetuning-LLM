SELECT id, hire_date FROM employees WHERE hire_date = (SELECT MAX(hire_date) FROM employees WHERE salary < 122000)
