SELECT id, job_title, dept_id, LENGTH(level) AS level_length FROM employees WHERE LENGTH(full_name) < 10 AND MONTH(hire_date) IN (3, 7, 10) ORDER BY level_length;
