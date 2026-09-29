SELECT id, job_title, salary, DENSE_RANK() OVER (PARTITION BY job_title ORDER BY salary DESC) AS dense_rnk FROM employees WHERE manager_id = 13
