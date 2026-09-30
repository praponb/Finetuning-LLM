SELECT level, AVG(salary) AS avg_salary, SUM(CASE WHEN job_title = 'Analyst' THEN 1 ELSE 0 END) AS analyst_count
FROM employees
GROUP BY level
ORDER BY level
