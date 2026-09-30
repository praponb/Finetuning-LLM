SELECT id, salary, NTILE(10) OVER (ORDER BY salary DESC, id) AS quartile
FROM employees
