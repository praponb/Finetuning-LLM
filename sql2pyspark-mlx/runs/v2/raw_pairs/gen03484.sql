SELECT job_title, full_name, CAST(is_remote AS INT) AS is_remote_flag FROM employees WHERE (YEAR(hire_date) >= 2025 AND level = 'L2') OR manager_id = 33 ORDER BY is_remote_flag;
