SELECT id, job_title, hire_date, RANK() OVER (PARTITION BY job_title ORDER BY hire_date) AS hire_date_rank FROM employees ORDER BY hire_date_rank DESC
