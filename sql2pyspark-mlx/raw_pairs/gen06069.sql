SELECT id, is_remote, ROUND(salary * 1.15, 2) AS adjusted_salary, CAST(salary AS INT) AS salary_int FROM employees WHERE email IS NOT NULL AND salary <= 137000 ORDER BY salary_int
