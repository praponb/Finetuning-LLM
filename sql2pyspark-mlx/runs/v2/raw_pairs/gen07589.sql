SELECT *, ROUND(salary, 2) AS salary_rounded, CAST(salary AS INT) AS salary_int FROM employees WHERE email IS NULL
