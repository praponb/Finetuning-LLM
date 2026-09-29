SELECT SUM(CASE WHEN job_title = 'Intern' THEN 1 ELSE 0 END) AS intern_count, COUNT(*) AS total_count FROM employees;
