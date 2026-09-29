SELECT level, ROUND((100 * SUM(CASE WHEN job_title = 'Designer' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_designer, AVG(salary) AS avg_salary FROM employees GROUP BY level;
