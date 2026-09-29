SELECT MONTH(hire_date) AS hire_month, COUNT(CASE WHEN level = 'L2' THEN 1 END) AS l2_count, ROUND((100 * SUM(CASE WHEN job_title = 'Designer' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_designer
FROM employees
GROUP BY MONTH(hire_date)
ORDER BY l2_count DESC;
