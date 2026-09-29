SELECT id, salary, PERCENT_RANK() OVER (ORDER BY salary) AS salary_pct_rank
FROM employees;
