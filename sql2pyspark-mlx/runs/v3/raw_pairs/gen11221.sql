SELECT id, job_title, hire_date, DENSE_RANK() OVER (PARTITION BY job_title ORDER BY hire_date DESC) AS dense_rnk FROM employees
