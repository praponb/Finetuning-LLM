SELECT id, salary, ROW_NUMBER() OVER (ORDER BY salary DESC, id) AS rn FROM employees WHERE salary < 125000
