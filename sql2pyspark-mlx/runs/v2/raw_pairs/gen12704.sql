SELECT id, manager_id, job_title, COUNT(*) OVER (PARTITION BY manager_id, job_title) AS manager_id_count FROM employees ORDER BY job_title
