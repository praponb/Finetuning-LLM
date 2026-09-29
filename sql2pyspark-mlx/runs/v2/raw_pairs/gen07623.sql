SELECT id, job_title, full_name, is_remote, CAST(is_remote AS INT) AS is_remote_flag FROM employees WHERE job_title IS NULL AND manager_id IN (4, 32, 37, 38) AND hire_date > '2025-06-01';
