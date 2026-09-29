SELECT id, hire_date, job_title, PERCENT_RANK() OVER (PARTITION BY MONTH(hire_date), job_title ORDER BY hire_date) AS hire_date_pct_rank FROM employees WHERE level <> 'L5' ORDER BY hire_date_pct_rank
