SELECT manager_id, MAX(hire_date) AS last_hire_date FROM employees WHERE hire_date < '2023-05-01' AND job_title = 'Designer' GROUP BY manager_id;
