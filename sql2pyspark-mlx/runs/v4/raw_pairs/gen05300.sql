SELECT level, COUNT(CASE WHEN job_title = 'Intern' THEN 1 END) AS intern_count FROM employees GROUP BY level ORDER BY intern_count DESC;
