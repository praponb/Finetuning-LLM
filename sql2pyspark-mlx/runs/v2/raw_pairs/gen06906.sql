SELECT id, dept_id, salary, PERCENT_RANK() OVER (PARTITION BY dept_id ORDER BY salary DESC) AS salary_pct_rank FROM employees
