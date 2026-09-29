SELECT level, is_remote, COUNT(CASE WHEN job_title = 'Manager' THEN 1 END) AS manager_count, ROUND(AVG(salary), 2) AS avg_salary FROM employees GROUP BY level, is_remote ORDER BY manager_count
