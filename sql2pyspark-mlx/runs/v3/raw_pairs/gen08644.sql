SELECT * FROM (SELECT level, job_title, MIN(hire_date) AS min_hire_date, COUNT(*) AS total_count FROM employees GROUP BY level, job_title) AS sub WHERE total_count >= 5 ORDER BY job_title;
