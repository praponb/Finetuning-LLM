SELECT id, job_title, salary, DENSE_RANK() OVER (PARTITION BY job_title ORDER BY salary DESC) AS salary_dense_rank FROM employees;
