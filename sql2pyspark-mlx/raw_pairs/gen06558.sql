SELECT id, job_title, salary, PERCENT_RANK() OVER (PARTITION BY job_title ORDER BY salary) AS salary_pct_rank FROM employees WHERE job_title IS NOT NULL ORDER BY salary_pct_rank
