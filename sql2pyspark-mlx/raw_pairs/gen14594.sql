SELECT is_remote, SUM(CASE WHEN job_title = 'Director' THEN 1 ELSE 0 END) AS director_count, ROUND(AVG(salary), 2) AS avg_salary
FROM employees
GROUP BY is_remote
HAVING ROUND(AVG(salary), 2) > 53;
